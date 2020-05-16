module dutils.message.client;

import std.uuid : UUID;
import std.exception : enforce;

import symmetry.api.rabbitmq;

import dutils.validation.validate : validate;
import dutils.validation.constraints : ValidateRequired, ValidateMinimumLength;
import dutils.random : randomUUID;
import dutils.message.message : Message;
import dutils.message.subscription : Subscription;
import dutils.message.exceptions : ConnectException, PublishException, DeclareQueueException;

struct ClientParameters {
  @ValidateRequired()
  @ValidateMinimumLength(1)
  string host = "localhost";

  @ValidateRequired()
  int port = 5672;

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

class Client {
  package amqp_connection_state_t connection;
  package Subscription[] subscriptions;
  private ClientParameters _parameters;
  private void delegate(Message)[UUID] requestCallbacks;
  private Subscription responseSubscription;
  private ushort lastSubscriptionChannel = 1;
  package QueueType[string] declaredQueues;

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

      auto status = amqp_socket_open(socket, this.parameters.host.toStringz, this.parameters.port);
      enforce(status == 0, "opening socket: " ~ amqp_error_string2(status).fromStringz);
      die_on_amqp_error(amqp_login(connection, "/".ptr, 0, 131072, 0,
          SaslMethod.plain, this.parameters.username.ptr, this.parameters.password.ptr),
          "Logging in");

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
      messageProperties.expiration = amqp_string((message.expiration.total!"msecs").to!string);
      messageProperties.correlation_id = amqp_string(message.correlationId.toString());
      messageProperties.type = amqp_string(message.type);

      if (message.replyToQueueName != "") {
        messageProperties._flags |= AMQP_BASIC_REPLY_TO_FLAG;
        messageProperties.reply_to = amqp_string(message.replyToQueueName);
      }

      if (message.token != "") {
        messageProperties._flags |= AMQP_BASIC_USER_ID_FLAG;
        messageProperties.user_id = amqp_string(message.token);
      }

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
      if (response.correlationId in this.requestCallbacks) {
        this.requestCallbacks[response.correlationId](response);
        this.requestCallbacks.remove(response.correlationId);
      }
    });
  }

  void request(string queueName, Message requestMessage, void delegate(Message) callback) {
    if (this.responseSubscription is null) {
      this.createResponseSubscription();
    }

    import core.time : Duration, seconds;

    if (requestMessage.expiration == Duration.zero) {
      requestMessage.expiration = 10.seconds;
    }

    if (requestMessage.correlationId.empty()) {
      requestMessage.correlationId = randomUUID();
    }

    import std.datetime.systime : Clock;

    const timeout = Clock.currTime + requestMessage.expiration;

    this.requestCallbacks[requestMessage.correlationId] = callback;

    requestMessage.replyToQueueName = this.responseSubscription.getQueueName();

    this.publish(queueName, requestMessage);
  }

  void update() {
    import std.algorithm : remove, countUntil;

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
 * AMQPClient#connect/isConnected/close - connect to the AMQP server
 */
unittest {
  auto client = new Client();
  assert(client.isConnected == true, "Expected client to be connected");

  client.close();
  assert(client.isConnected == false, "Expected client to not be connected");

  client.connect();
  assert(client.isConnected == true, "Expected client to be connected");

  ConnectException connectionError;
  try {
    client = new Client(ClientParameters("badhostname"));
  } catch (ConnectException exception) {
    connectionError = exception;
  }

  assert(connectionError.message
      == "Unable to connect to AMQP 0-9-1 service at badhostname:5672 with username guest");
}

/**
 * AMQPClient#subscribe/publish - subscribe/publish to a queue on an AMQP server
 */
unittest {
  import core.time : seconds;

  for (int index; index < 10; index++) {
    auto client = new Client();
    assert(client.isConnected == true, "Expected client to be connected");

    auto service = new Client();
    assert(service.isConnected == true, "Expected service to be connected");

    struct DummyMessage {
      string story;
    }

    auto dummy = DummyMessage("Lorem ipsum dolor sit amet, consectetur adipisicing elit." ~ randomUUID()
        .toString());
    auto message = Message.from(dummy);
    message.expiration = 500.seconds;

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

    assert(recievedMessage.type != "Error", "Expected message type to not be Error");
    assert(recievedMessage.correlationId == message.correlationId,
        "Expeceted correlationId to match");
    assert(recievedMessage.payload["story"].get!string == dummy.story,
        "Expeceted payload to be Lorem ipsum...");
  }
}

/**
 * AMQPClient#request - request server/client
 */
unittest {
  import core.time : seconds, msecs;
  import core.thread : Thread;
  import dutils.data.bson : BSON;

  auto client = new Client();
  client.connect();

  auto service = new Client();
  service.connect();

  service.subscribe("testservice", (Message request) {
    if (request.type == "Ping") {
      struct Pong {
        string story;
      }

      auto payload = Pong("Playing ping pong with " ~ request.payload["name"].get!string);
      auto response = Message.from(payload, request.correlationId);
      service.publish(request.replyToQueueName, response);
    } else {
      struct Error {
        string message;
      }

      auto payload = Error("Unknown message type: " ~ request.type);
      auto response = Message.from(payload, request.correlationId);
      service.publish(request.replyToQueueName, response);
    }
  });

  struct Ping {
    string name;
  }

  auto payload = Ping("Anna");
  auto message = Message.from(payload);

  Message response;
  client.request("testservice", message, (Message requestResponse) {
    response = requestResponse;
  });

  auto badMessage = Message();
  badMessage.type = "BadType";
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

  assert(response.type == "Pong", "Expected message type to be Pong");
  assert(response.correlationId == message.correlationId, "Expected correlationId to match");
  assert(response.payload["story"].get!string == "Playing ping pong with Anna",
      "Expected payload to be: Playing ping pong with Anna");

  assert(errorResponse.type == "Error", "Expected message type to be Error");
  assert(errorResponse.correlationId == badMessage.correlationId, "Expected correlationId to match");
  assert(errorResponse.payload["message"].get!string == "Unknown message type: BadType",
      "Expected payload to be: Unknown message type: BadType");
}
