module dutils.message.subscription;

import core.thread : Fiber;
import std.uuid : UUID;
import std.conv : to;

import symmetry.api.rabbitmq;

import dutils.data.bson : BSON;
import dutils.message.message : Message, ResponseStatus;
import dutils.message.client : Client, QueueType;
import dutils.message.exceptions : SubscribeQueueException,
  MessageBodyException, MessageHeaderException;

class Subscription {
  private Client client;
  private string queueName;
  private QueueType queueType;
  private ushort channel;
  package Fiber fiber;
  private void delegate(Message) callback;
  private bool _isClosed = true;

  this(Client client, string queueName, QueueType queueType, ushort channel,
      void delegate(Message) callback) {
    this.client = client;
    this.queueName = queueName;
    this.queueType = queueType;
    this.channel = channel;
    this.callback = callback;
    this.fiber = new Fiber(&this.handler);
    this.fiber.call();
  }

  void close() {
    try {
      if (!this._isClosed) {
        die_on_amqp_error(amqp_channel_close(this.client.connection,
            this.channel, ReplySuccess), "Closing channel");
      }
    } catch (Exception exception) {
    }

    this._isClosed = true;
  }

  @property bool isClosed() {
    return this._isClosed;
  }

  string getQueueName() {
    return this.queueName;
  }

  private void handler() {
    import std.string : fromStringz;

    if (!(this.queueName in this.client.declaredQueues)) {
      this.client.declareQueue(this.queueName, this.queueType);
    }

    try {
      amqp_channel_open(this.client.connection, this.channel);
      die_on_amqp_error(amqp_get_rpc_reply(this.client.connection), "Opening channel");

      amqp_basic_consume(this.client.connection, this.channel,
          amqp_string(this.queueName), cast(amqp_bytes_t) amqp_empty_bytes, 0,
          0, 0, cast(amqp_table_t) amqp_empty_table);
      die_on_amqp_error(amqp_get_rpc_reply(this.client.connection), "Consuming");
    } catch (Exception exception) {
      throw new SubscribeQueueException(this.queueName, exception);
    }

    this._isClosed = false;

    while (!this.isClosed) {
      Message message;
      bool gotException = false;
      amqp_envelope_t envelope;

      try {
        amqp_rpc_reply_t reply;

        amqp_maybe_release_buffers(this.client.connection);

        timeval timeout;
        timeout.tv_usec = 100_000;
        reply = amqp_consume_message(this.client.connection, &envelope, &timeout, 0);

        if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION
            && reply.library_error == AMQP_STATUS_TIMEOUT) {
          Fiber.yield();
          continue;
        } else if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
          if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
            throw new Exception(amqp_error_string2(reply.library_error).fromStringz.to!string);
          } else if (reply.reply_type == AMQP_RESPONSE_SERVER_EXCEPTION) {
            throw new Exception(amqp_error_string2(reply.reply.id).fromStringz.to!string);
          }

          throw new Exception("Unknown error from AMQP service");
        }

        message = this.envelopeToMessage(&envelope);
        this.callback(message);
      } catch (Exception exception) {
        this.client.logger.error(exception);
        gotException = exception is null;
      }

      // TODO: evaluate if this extra message re-delivery try is working as intended, should there be more retrys?
      if (gotException == true) {
        amqp_basic_reject(this.client.connection, this.channel,
            envelope.delivery_tag, envelope.redelivered == false);
      } else {
        amqp_basic_ack(this.client.connection, this.channel, envelope.delivery_tag, false);
      }

      amqp_destroy_envelope(&envelope);

      Fiber.yield();
    }

    this.close();
  }

  private Message envelopeToMessage(amqp_envelope_t* envelope) {
    import core.time : msecs;

    auto message = Message();

    if (envelope.message.properties._flags & AMQP_BASIC_CORRELATION_ID_FLAG) {
      message.correlationId = UUID(envelope.message.properties.correlation_id.asString());
    }

    if (envelope.message.properties._flags & AMQP_BASIC_TYPE_FLAG) {
      message.type = envelope.message.properties.type.asString();
    }

    if (envelope.message.properties._flags & AMQP_BASIC_REPLY_TO_FLAG) {
      const value = envelope.message.properties.reply_to.asString();
      if (value != "") {
        message.replyToQueueName = value;
      }
    }

    if (envelope.message.properties._flags & AMQP_BASIC_HEADERS_FLAG) {
      import std.datetime.systime : SysTime;

      auto headers = envelope.message.properties.headers;

      for (int index = 0; index < headers.num_entries; index++) {
        auto entry = headers.entries[index];
        auto key = entry.key.asString();

        if (key == "created") {
          try {
            message.created = SysTime.fromISOExtString(entry.value.value.bytes.asString());
          } catch (Exception exception) {
            throw new MessageHeaderException(key, message.type);
          }
        } else if (key == "expires") {
          try {
            message.expires = SysTime.fromISOExtString(entry.value.value.bytes.asString());
          } catch (Exception exception) {
            throw new MessageHeaderException(key, message.type);
          }
        } else if (key == "token") {
          message.token = entry.value.value.bytes.asString();
        } else if (key == "responseStatus") {
          try {
            message.responseStatus = (cast(int) entry.value.value.u16).to!ResponseStatus;
          } catch (Exception exception) {
            throw new MessageHeaderException(key, message.type);
          }
        }
      }

      const value = envelope.message.properties.user_id.asString();
      if (value != "") {
        message.token = value;
      }
    }

    if (envelope.message.body_.len > 0) {
      try {
        auto body = cast(immutable(ubyte)[]) fromBytes(envelope.message.body_.bytes,
            envelope.message.body_.len);
        message.payload = BSON(BSON.Type.object, body);
      } catch (Exception exception) {
        throw new MessageBodyException(message.type);
      }
    }

    return message;
  }
}

private char[] fromBytes(void* ptr, ulong len) {
  return (cast(char*) ptr)[0 .. len].dup;
}

private string asString(amqp_bytes_t bytes) {
  return (cast(char*) bytes.bytes)[0 .. bytes.len].idup;
}
