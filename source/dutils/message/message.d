module dutils.message.message;

import core.time : Duration, seconds;
import std.uuid : UUID;

import dutils.data.bson : BSON;
import dutils.validation.validate : validate;
import dutils.random : randomUUID;
import dutils.message.exceptions : TypeMismatchException;

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
    import dutils.data.bson : serializeToBSON;

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

  T to(T)() {
    import dutils.data.bson : populateFromBSON;

    if (T.stringof != this.type) {
      throw new TypeMismatchException(T.stringof, this.type);
    }

    T result;
    populateFromBSON(result, this.payload);

    return result;
  }

  @property bool isError() {
    return this.type == "Error";
  }

  @property bool isEmpty() {
    return this.type == "";
  }
}

/**
 * Message#from/to
 */
unittest {
  struct Page {
    string title;
    string body;
  }

  auto expectedOutput = Page("A title", "Lorem ipsum dolor sit amet, consectetur...");

  auto message = Message.from(expectedOutput);
  auto output = message.to!Page();

  assert(output.title == expectedOutput.title, "Expected title to match");
  assert(output.body == expectedOutput.body, "Expected body to match");
}

/**
 * Message#to - bad type
 */
unittest {
  struct Page {
    string title;
    string body;
  }

  struct Author {
    string name;
  }

  auto expectedOutput = Page("A title", "Lorem ipsum dolor sit amet, consectetur...");

  auto message = Message.from(expectedOutput);

  TypeMismatchException gotException;
  try {
    auto output = message.to!Author();
  } catch (TypeMismatchException exception) {
    gotException = exception;
  }

  assert(gotException.msg == "Expected type Author but got Page");
}
