module dutils.message.subscription;

import core.thread : Fiber;
import std.uuid : UUID;
import std.conv : to;

import symmetry.api.rabbitmq;

import dutils.data.bson : BSON;
import dutils.message.message : Message;
import dutils.message.client : Client, QueueType;
import dutils.message.exceptions : SubscribeQueueException, MessageBodyException;

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

        // TODO: add handling for redelivery and handling for when the subscribe callback crashes
        amqp_basic_ack(this.client.connection, this.channel, envelope.delivery_tag, false);
      } catch (Exception exception) {
        message = this.errorToMessage(exception, &envelope);
      }

      amqp_destroy_envelope(&envelope);

      this.callback(message);

      Fiber.yield();
    }

    this.close();
  }

  private Message errorToMessage(Exception exception, amqp_envelope_t* envelope) {
    auto message = Message();

    message.type = "Error";

    if (envelope.message.properties._flags & AMQP_BASIC_CORRELATION_ID_FLAG) {
      message.correlationId = UUID(fromBytes(envelope.message.properties.correlation_id.bytes,
          envelope.message.properties.correlation_id.len).to!string);
    }

    message.payload = BSON(exception.message.to!string);

    return message;
  }

  private Message envelopeToMessage(amqp_envelope_t* envelope) {
    import core.time : msecs;

    auto message = Message();

    if (envelope.message.properties._flags & AMQP_BASIC_EXPIRATION_FLAG) {
      auto value = fromBytes(envelope.message.properties.expiration.bytes,
          envelope.message.properties.expiration.len).to!string;
      message.expiration = msecs(value.to!long);
    }

    if (envelope.message.properties._flags & AMQP_BASIC_CORRELATION_ID_FLAG) {
      message.correlationId = UUID(fromBytes(envelope.message.properties.correlation_id.bytes,
          envelope.message.properties.correlation_id.len).to!string);
    }

    if (envelope.message.properties._flags & AMQP_BASIC_TYPE_FLAG) {
      message.type = fromBytes(envelope.message.properties.type.bytes,
          envelope.message.properties.type.len).to!string;
    }

    if (envelope.message.properties._flags & AMQP_BASIC_REPLY_TO_FLAG) {
      const value = fromBytes(envelope.message.properties.reply_to.bytes,
          envelope.message.properties.reply_to.len).to!string;
      if (value != "") {
        message.replyToQueueName = value;
      }
    }

    if (envelope.message.properties._flags & AMQP_BASIC_USER_ID_FLAG) {
      const value = fromBytes(envelope.message.properties.user_id.bytes,
          envelope.message.properties.user_id.len).to!string;
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
