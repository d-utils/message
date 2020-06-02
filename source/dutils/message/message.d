module dutils.message.message;

import core.time : Duration, seconds;
import std.datetime.systime : SysTime, Clock;
import std.uuid : UUID;

import dutils.data.bson : BSON, serializeToBSON;
import dutils.data.json : JSON;
import dutils.validation.validate : validate, ValidationError;
import dutils.random : randomUUID;
import dutils.message.exceptions : TypeMismatchException;

struct Message {
  string type;
  BSON payload;
  string token;
  string replyToQueueName;
  UUID correlationId;
  SysTime created;
  SysTime expires;
  ResponseStatus responseStatus = ResponseStatus.NOT_APPLICABLE;

  static Message opCall() {
    auto message = Message.init;

    message.correlationId = randomUUID();
    message.expires = Clock.currTime() + 10.seconds;
    message.created = Clock.currTime();

    return message;
  }

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

    if (!correlationId.empty()) {
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

  @property bool isEmpty() {
    return this.type == "";
  }

  @property bool hasExpired() {
    return Clock.currTime() > this.expires;
  }

  Message createResponse(T)(T payload) {
    import std.traits : getUDAs;

    auto status = ResponseStatus.OK;
    auto udas = getUDAs!(T, SetResponseStatus);
    static if (udas.length > 0) {
      status = udas[0].status;
    }

    return this.createResponse(status, payload);
  }

  Message createResponse(T)(ResponseStatus responseStatus, T payload) {
    validate(payload);

    auto message = Message();

    message.type = T.stringof;
    message.payload = serializeToBSON(payload);
    message.token = this.token;
    message.correlationId = this.correlationId;
    message.responseStatus = responseStatus;
    message.expires = this.expires;

    return message;
  }

  void setToExpireAfter(Duration duration) {
    this.expires = Clock.currTime() + duration;
  }

  JSON toJSON() {
    return serializeToBSON(this).toJSON();
  }

  string toString() {
    return this.toJSON().toString();
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

/**
 * Message#createResponse - verify that response status is set
 */
unittest {
  import std.conv : to;

  struct Ping {
    string message;
  }

  auto ping = Ping("Lorem ipsum...");
  auto request = Message.from(ping);
  auto response = request.createResponse(ResponseForbidden());

  assert(response.type == "ResponseForbidden",
      "Expected response.type to equal ResponseForbidden, got " ~ response.type);
  assert(response.responseStatus == ResponseStatus.FORBIDDEN,
      "Expected responseStatus to be ResponseStatus.FORBIDDEN got "
      ~ response.responseStatus.to!string);
  assert(response.correlationId == request.correlationId,
      "response.correlationId should equal request.correlationId");
}

/**
 * The standard message type to send back on success when no response data is needed
 */
@SetResponseStatus(ResponseStatus.OK)
struct ResponseOK {
}

/**
 * The standard message type to send back when the request message has an error,
 * being less specific than ResponseInvalid or ResponseBadType it is better
 * to try to use one of them as much as possible over this payload.
 */
@SetResponseStatus(ResponseStatus.BAD_REQUEST)
struct ResponseBadRequest {
  /**
   * An array with the validation errors
   */
  string message;
}

/**
 * The standard message type to send back when there are validation errors from the data that the client sent
 */
@SetResponseStatus(ResponseStatus.INVALID)
struct ResponseInvalid {
  /**
   * An array with the validation errors
   */
  ValidationError[] errors;
}

/**
 * The standard message type to send back when the type recieved is not supported
 */
@SetResponseStatus(ResponseStatus.BAD_TYPE)
struct ResponseBadType {
  /**
   * The name of the bad type
   */
  string type;

  /**
   * An array with the expected types
   */
  string[] supportedTypes;
}

/**
 * The standard message type to send back when the supplied token is not valid or lacks required permissions
 */
@SetResponseStatus(ResponseStatus.FORBIDDEN)
struct ResponseForbidden {
}

/**
 * The standard message type to send back when there was an internal error, this message will not reveal any details
 * but best practice is to also log the error on the service side.
 */
@SetResponseStatus(ResponseStatus.INTERNAL_ERROR)
struct ResponseInternalError {
}

/**
 * The standard message type to send back when the service did not produce a response message before the timeout.
 */
@SetResponseStatus(ResponseStatus.TIMEOUT)
struct ResponseTimeout {
}

struct SetResponseStatus {
  ResponseStatus status;
}

enum ResponseStatus {
  NOT_APPLICABLE = 0,
  OK = 200,
  BAD_REQUEST = 400,
  FORBIDDEN = 403,
  BAD_TYPE = 470,
  INVALID = 471,
  INTERNAL_ERROR = 500,
  TIMEOUT = 601
}
