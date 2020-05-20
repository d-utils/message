module dutils.message.exceptions;

import std.conv : to;
import core.time : Duration;

class ConnectException : Exception {
  this(string host, int port, string username, string file = __FILE__, size_t line = __LINE__) {
    super("Unable to connect to AMQP 0-9-1 service at " ~ host ~ ":" ~ port.to!string ~ " with username " ~ username,
        file, line);
  }
}

class PublishException : Exception {
  this(string type, string queueName, string file = __FILE__, size_t line = __LINE__) {
    super("Failed to publish " ~ type ~ " message to queue \"" ~ queueName ~ "\" on  0-9-1 service",
        file, line);
  }
}

class DeclareQueueException : Exception {
  this(string queueName, string file = __FILE__, size_t line = __LINE__) {
    super("Failed to declare queue \"" ~ queueName ~ "\" on  0-9-1 service", file, line);
  }
}

class SubscribeQueueException : Exception {
  this(string queueName, Exception originalException, string file = __FILE__, size_t line = __LINE__) {
    super("Failed to subscribe to queue \"" ~ queueName
        ~ "\" on  0-9-1 service. Original exception: " ~ originalException.message.to!string,
        file, line);
  }
}

class RequestException : Exception {
  this(string type, string queueName, string file = __FILE__, size_t line = __LINE__) {
    super("Request " ~ type ~ " on queue \"" ~ queueName ~ "\" failed on  0-9-1 service",
        file, line);
  }
}

class TimeoutRequestException : Exception {
  this(string type, Duration expiration, string queueName,
      string file = __FILE__, size_t line = __LINE__) {
    super("Request " ~ type ~ " on queue \"" ~ queueName ~ "\" reached the timeout limit of " ~ expiration.toString()
        ~ " on  0-9-1 service", file, line);
  }
}

class MessageBodyException : Exception {
  this(string type, string file = __FILE__, size_t line = __LINE__) {
    super("Failed to parse message body for message of type " ~ type, file, line);
  }
}

class TypeMismatchException : Exception {
  this(string expectedType, string type, string file = __FILE__, size_t line = __LINE__) {
    super("Expected type " ~ expectedType ~ " but got " ~ type, file, line);
  }
}
