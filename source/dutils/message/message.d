module dutils.message.message;

import core.time : Duration, seconds;
import std.uuid : UUID;

import dutils.data.bson : BSON, serializeToBSON;
import dutils.validation.validate : validate;
import dutils.random : randomUUID;

struct Message {
  string type;
  BSON payload;
  string token;
  Duration expiration;
  string replyToQueueName;
  UUID correlationId;

  static Message from(T)(T payload) {
    return Message.from(payload, "");
  }

  static Message from(T)(T payload, string token) {
    return Message.from(payload, token, randomUUID());
  }

  static Message from(T)(T payload, UUID correlationId) {
    return Message.from(payload, "", correlationId);
  }

  static Message from(T)(T payload, string token, UUID correlationId) {
    validate(payload);

    auto message = Message();

    message.type = T.stringof;
    message.payload = serializeToBSON(payload);
    message.token = token;
    message.expiration = 10.seconds;

    if (correlationId.empty()) {
      message.correlationId = randomUUID();
    } else {
      message.correlationId = correlationId;
    }

    return message;
  }

  @property bool isError() {
    return this.type == "Error";
  }

  @property bool isEmpty() {
    return this.type == "";
  }
}
