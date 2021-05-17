package cn.edu.thssdb.exception;

import java.io.IOException;

/**
 * Thrown when serialization from files failed.
 */
public class SerializationException extends IOException {
  public SerializationException(String message) {
    super(message);
  }

  public SerializationException(String message, Exception cause) {
    super(message, cause);
  }

  public SerializationException(Exception cause) {
    super(cause);
  }
}
