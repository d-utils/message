module dutils.message.client;

import std.uuid : UUID;
import std.exception : enforce;

import std.datetime.systime : Clock;
import symmetry.api.rabbitmq;
import util.log : Log, stderrLogger, stdoutLogger, LogLevel, orBelow;

import dutils.validation.validate : validate;
import dutils.validation.constraints : ValidateRequired, ValidateMinimumLength,
  ValidateMaximum, ValidateMinimum;
import dutils.random : randomUUID;
import dutils.message.message : Message, ResponseStatus, SetResponseStatus, ResponseTimeout;
import dutils.message.subscription : Subscription;
import dutils.message.exceptions : ConnectException, PublishException, DeclareQueueException;

struct ClientParameters {
  @ValidateRequired()
  @ValidateMinimumLength(1)
  string host = "localhost";

  @ValidateRequired()
  @ValidateMinimum!ushort(1) @ValidateMaximum!ushort(65535) ushort port = 5672;

  @ValidateRequired()
  @ValidateMinimumLength(1)
  string username = "guest";

  @ValidateRequired() @ValidateMinimumLength(1)
  string password = "guest";
}

enum QueueType {
  DURABLE,
  AUTO_DELETE
}

private struct ClientState {
  bool closeAfterUpdate = false;
  bool updateRunning = false;
}

private struct Request {
  Message message;
  void delegate(Message) callback;
}

class Client {
  package amqp_connection_state_t connection;
  package Subscription[] subscriptions;
  private ClientParameters _parameters;
  private Request[UUID] requests;
  private Subscription responseSubscription;
  private ushort lastSubscriptionChannel = 1;
  package QueueType[string] declaredQueues;
  package Log logger;
  private ClientState state;

  @property ClientParameters parameters() {
    return this._parameters;
  }

  @property bool isConnected() {
    return this.connection != null;
  }

  this() {
    this(ClientParameters());
  }

  this(const ClientParameters parameters) {
    this.logger = Log(stderrLogger, stdoutLogger(LogLevel.info.orBelow));

    validate(parameters);
    this._parameters = parameters;
    this.connect();
  }

  ~this() {
    this.close();
  }

  void connect() {
    this.close();

    amqp_connection_state_t connection;

    try {
      // TODO: add SSL support
      connection = amqp_new_connection();

      auto socket = amqp_tcp_socket_new(connection);
      enforce(socket !is null, "creating tcp socket");

      import std.string : toStringz, fromStringz;
      import std.conv : to;

      auto status = amqp_socket_open(socket, this.parameters.host.toStringz,
          this.parameters.port.to!int);
      enforce(status == 0, "opening socket: " ~ amqp_error_string2(status).fromStringz);

      auto username = this.parameters.username;
      auto password = this.parameters.password;
      die_on_amqp_error(amqp_login(connection, "/", 0, 131072, 0,
          SaslMethod.plain, username.toStringz, password.toStringz), "Logging in");

      amqp_channel_open(connection, 1);
      die_on_amqp_error(amqp_get_rpc_reply(connection), "Opening channel");
    } catch (Exception exception) {
      throw new ConnectException(this.parameters.host, this.parameters.port,
          this.parameters.username);
    }

    this.connection = connection;
  }

  void close() {
    if (!this.isConnected) {
      return;
    }

    if (this.state.updateRunning) {
      this.state.closeAfterUpdate = true;
      return;
    }

    try {
      foreach (subscription; this.subscriptions) {
        subscription.close();
      }

      if (this.connection) {
        die_on_amqp_error(amqp_channel_close(this.connection, 1, ReplySuccess), "Closing channel");
        die_on_amqp_error(amqp_connection_close(this.connection, ReplySuccess),
            "Closing connection");
        die_on_error(amqp_destroy_connection(this.connection), "Ending connection");
      }
    } catch (Exception exception) {
      // TODO: add error handler
    }

    this.responseSubscription = null;
    this.connection = null;
    this.state.closeAfterUpdate = false;
  }

  void publish(string queueName, Message message) {
    this.publish(queueName, message, QueueType.AUTO_DELETE);
  }

  void publish(string queueName, ref Message message) {
    this.publish(queueName, message, QueueType.AUTO_DELETE);
  }

  void publish(string queueName, ref Message message, QueueType queueType) {
    if (!this.isConnected) {
      this.connect();
    }

    if (!(queueName in this.declaredQueues)) {
      this.declareQueue(queueName, queueType);
    }

    try {
      import std.conv : to;

      amqp_basic_properties_t messageProperties;
      messageProperties._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG
        | AMQP_BASIC_EXPIRATION_FLAG | AMQP_BASIC_CORRELATION_ID_FLAG | AMQP_BASIC_TYPE_FLAG;

      messageProperties.content_type = amqp_string("application/octet-stream");
      messageProperties.delivery_mode = 2; /* persistent delivery mode */
      messageProperties.correlation_id = amqp_string(message.correlationId.toString());
      messageProperties.type = amqp_string(message.type);

      auto expiration = message.expires - Clock.currTime();
      messageProperties.expiration = amqp_string((expiration.total!"msecs").to!string);

      if (message.replyToQueueName != "") {
        messageProperties._flags |= AMQP_BASIC_REPLY_TO_FLAG;
        messageProperties.reply_to = amqp_string(message.replyToQueueName);
      }

      /**
       * Example in C for constructing header entries:
       * https://github.com/alanxz/rabbitmq-c/blob/a65c64c0efd883f3e200bd8831ad3ca066ea523c/tests/test_merge_capabilities.c#L163
       */
      import std.string : toStringz;

      messageProperties._flags |= AMQP_BASIC_HEADERS_FLAG;

      int headersIndex = -1;
      amqp_table_entry_t[4] headerEntries;

      headersIndex++;
      headerEntries[headersIndex].key = amqp_cstring_bytes("created");
      headerEntries[headersIndex].value.kind = AMQP_FIELD_KIND_UTF8;
      headerEntries[headersIndex].value.value.bytes = amqp_cstring_bytes(
          message.created.toISOExtString().toStringz);

      headersIndex++;
      headerEntries[headersIndex].key = amqp_cstring_bytes("expires");
      headerEntries[headersIndex].value.kind = AMQP_FIELD_KIND_UTF8;
      headerEntries[headersIndex].value.value.bytes = amqp_cstring_bytes(
          message.expires.toISOExtString().toStringz);

      if (message.token != "") {
        headersIndex++;
        headerEntries[headersIndex].key = amqp_cstring_bytes("token");
        headerEntries[headersIndex].value.kind = AMQP_FIELD_KIND_UTF8;
        headerEntries[headersIndex].value.value.bytes = amqp_cstring_bytes(message.token.toStringz);
      }

      if (message.responseStatus != ResponseStatus.NOT_APPLICABLE) {
        headersIndex++;
        headerEntries[headersIndex].key = amqp_cstring_bytes("responseStatus");
        headerEntries[headersIndex].value.kind = AMQP_FIELD_KIND_U16;
        headerEntries[headersIndex].value.value.u16 = message.responseStatus.to!ushort;
      }

      messageProperties.headers.num_entries = headersIndex + 1;
      messageProperties.headers.entries = headerEntries.ptr;

      amqp_bytes_t payload;
      auto data = cast(char[]) message.payload.data;
      payload.len = data.length;
      payload.bytes = data.ptr;

      die_on_error(amqp_basic_publish(this.connection, 1, amqp_string("amq.direct"),
          amqp_string(queueName), 0, 0, &messageProperties, payload), "Publishing");

    } catch (Exception exception) {
      throw new PublishException(message.type, queueName);
    }
  }

  Subscription subscribe(string queueName, void delegate(Message) callback) {
    return this.subscribe(queueName, callback, QueueType.AUTO_DELETE);
  }

  Subscription subscribe(string queueName, void delegate(Message) callback, QueueType queueType) {
    if (!this.isConnected) {
      this.connect();
    }

    auto subscription = new Subscription(this, queueName, queueType,
        ++this.lastSubscriptionChannel, callback);
    this.subscriptions ~= subscription;

    return subscription;
  }

  private void createResponseSubscription() {
    auto responseQueueName = "responselistener-" ~ randomUUID().toString();

    this.responseSubscription = this.subscribe(responseQueueName, (Message response) {
      if (response.correlationId in this.requests) {
        auto request = this.requests[response.correlationId];

        try {
          if (request.message.hasExpired == false) {
            request.callback(response);
          } else {
            request.callback(request.message.createResponse(ResponseTimeout()));
          }
        } catch (Exception exception) {
          this.logger.error(exception);
        }

        this.requests.remove(response.correlationId);
      }
    });
  }

  void request(string queueName, Message requestMessage, void delegate(Message) callback) {
    this.request(queueName, requestMessage, callback);
  }

  void request(string queueName, ref Message requestMessage, void delegate(Message) callback) {
    if (this.responseSubscription is null) {
      this.createResponseSubscription();
    }

    if (requestMessage.correlationId.empty()) {
      requestMessage.correlationId = randomUUID();
    }

    requestMessage.replyToQueueName = this.responseSubscription.getQueueName();

    this.requests[requestMessage.correlationId] = Request(requestMessage, callback);

    this.publish(queueName, requestMessage);
  }

  void update() {
    import std.algorithm : remove, countUntil;

    this.state.updateRunning = true;

    foreach (subscription; this.subscriptions) {
      if (subscription.isClosed) {
        auto index = this.subscriptions.countUntil(this);
        if (index >= 0) {
          this.subscriptions.remove(index);
        }
      } else {
        subscription.fiber.call();
      }
    }

    foreach (request; this.requests) {
      if (request.message.hasExpired == true) {
        try {
          request.callback(request.message.createResponse(ResponseTimeout()));
        } catch (Exception exception) {
          this.logger.error(exception);
        }
        this.requests.remove(request.message.correlationId);
      }
    }

    this.state.updateRunning = false;
    if (this.state.closeAfterUpdate) {
      this.close();
    }
  }

  void keepUpdating() {
    import core.thread : Thread;
    import core.time : msecs;

    while (this.isConnected) {
      this.update();
      Thread.sleep(1.msecs);
    }
  }

  package void declareQueue(string queueName, QueueType type) {
    try {
      {
        amqp_queue_declare_ok_t* result = amqp_queue_declare(this.connection, 1, amqp_string(queueName), 0,
            type == QueueType.DURABLE, 0, type == QueueType.AUTO_DELETE,
            cast(amqp_table_t) amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(this.connection), "Declaring queue");
        auto generatedQueueName = amqp_bytes_malloc_dup(result.queue);
        enforce(generatedQueueName.bytes !is null, "Out of memory while copying queue name");

        amqp_queue_bind(this.connection, 1, generatedQueueName, amqp_string("amq.direct"),
            amqp_string(queueName), cast(amqp_table_t) amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(this.connection), "Binding queue");
      }

      this.declaredQueues[queueName] = type;
    } catch (Exception exception) {
      throw new DeclareQueueException(queueName);
    }
  }
}

/**
 * Client#connect/isConnected/close - connect to the AMQP server
 */
unittest {
  import dutils.testing : assertEqual;

  auto client = new Client();
  assertEqual(client.isConnected, true);

  client.close();
  assertEqual(client.isConnected, false);

  client.connect();
  assertEqual(client.isConnected, true);

  ConnectException connectionError;
  try {
    client = new Client(ClientParameters("badhostname"));
  } catch (ConnectException exception) {
    connectionError = exception;
  }

  assertEqual(connectionError.message,
      "Unable to connect to AMQP 0-9-1 service at badhostname:5672 with username guest");
}

/**
 * Client#subscribe/publish - subscribe/publish to a queue on an AMQP server
 */
unittest {
  import dutils.testing : assertEqual;

  for (int index; index < 10; index++) {
    auto client = new Client();
    assertEqual(client.isConnected, true);

    auto service = new Client();
    assertEqual(service.isConnected, true);

    struct DummyMessage {
      string story;
    }

    auto dummy = DummyMessage("Lorem ipsum dolor sit amet, consectetur adipisicing elit." ~ randomUUID()
        .toString());
    auto message = Message.from(dummy);

    if (index % 2 == 1) {
      client.publish("testqueue", message);
    }

    Message recievedMessage;
    service.subscribe("testqueue", (Message recievedMessageTemp) {
      recievedMessage = recievedMessageTemp;
    });

    if (index % 2 == 0) {
      client.publish("testqueue", message);
    }

    while (recievedMessage.isEmpty) {
      service.update();
    }

    service.close();
    client.close();

    assertEqual(recievedMessage.type, "DummyMessage");
    assertEqual(recievedMessage.correlationId.toString(), message.correlationId.toString());
    assertEqual(recievedMessage.payload["story"].get!string, dummy.story);
  }
}

/**
 * Client#request - request server/client
 */
unittest {
  import std.conv : to;

  import dutils.testing : assertEqual;
  import dutils.data.bson : BSON;
  import dutils.message.message : ResponseBadType;

  auto client = new Client();
  auto service = new Client();

  service.subscribe("testservice", (Message request) {
    if (request.type == "Ping") {
      @SetResponseStatus(ResponseStatus.OK)
      struct Pong {
        string story;
      }

      auto payload = Pong("Playing ping pong with " ~ request.payload["name"].get!string);
      service.publish(request.replyToQueueName, request.createResponse(payload));
    } else {
      auto payload = ResponseBadType(request.type, ["Ping"]);
      service.publish(request.replyToQueueName, request.createResponse(payload));
    }
  });

  struct Ping {
    string name;
  }

  auto payload = Ping("Anna");
  auto message = Message.from(payload);
  message.token = "this is a placeholder token";

  Message response;
  client.request("testservice", message, (Message requestResponse) {
    response = requestResponse;
  });

  auto badMessage = Message();
  badMessage.type = "ThisIsABadType";
  badMessage.correlationId = randomUUID();

  Message errorResponse;
  client.request("testservice", badMessage, (Message requestResponse) {
    errorResponse = requestResponse;
  });

  while (response.isEmpty || errorResponse.isEmpty) {
    service.update();
    client.update();
  }

  service.close();
  client.close();

  assertEqual(response.type, "Pong");
  assertEqual(response.correlationId.toString(), message.correlationId.toString());
  assertEqual(response.payload["story"].get!string, "Playing ping pong with Anna");
  assertEqual(response.responseStatus, ResponseStatus.OK);
  assertEqual(response.token, "this is a placeholder token");

  assertEqual(errorResponse.type, "ResponseBadType");
  assertEqual(errorResponse.correlationId.toString(), badMessage.correlationId.toString());
  assertEqual(errorResponse.payload["supportedTypes"][0].get!string, "Ping");
  assertEqual(errorResponse.responseStatus, ResponseStatus.BAD_TYPE);
}

/**
 * Client#request - request timeout
 */
unittest {
  import std.conv : to;
  import core.time : msecs;

  import dutils.testing : assertEqual;

  auto client = new Client();

  struct Ping {
    string name;
  }

  auto payload = Ping("Anna");
  auto message = Message.from(payload);
  message.token = "this is a placeholder token";
  message.setToExpireAfter(1000.msecs);

  Message response;
  client.request("testservicetimeout", message, (Message requestResponse) {
    response = requestResponse;
  });

  while (response.isEmpty) {
    client.update();
  }

  client.close();

  assertEqual(response.type, "ResponseTimeout");
  assertEqual(response.correlationId.toString(), message.correlationId.toString());
  assertEqual(response.responseStatus, ResponseStatus.TIMEOUT);
  assertEqual(response.token, "this is a placeholder token");
}

/**
 * Client#keepUpdating - should loop until
 */
unittest {
  import dutils.data.bson : BSON;

  import dutils.testing : assertEqual;

  auto client = new Client();

  struct Hello {
    string from;
  }

  auto gotMessage = false;

  Subscription subscription;

  subscription = client.subscribe("testservice", (Message message) {
    if (message.type == "Hello" && message.payload["from"].get!string == "Pelle") {
      gotMessage = true;
      client.close();
    }
  });

  auto message = Message.from(Hello("Pelle"));
  client.publish("testservice", message);

  client.keepUpdating();

  assertEqual(gotMessage, true);
}
