package cn.edu.thssdb.exception;

import java.io.IOException;

/**
 * Thrown when deserialization from files failed.
 */
public class DeserializationException extends IOException {
  public DeserializationException(String message) {
    super(message);
  }

  public DeserializationException(String message, Exception cause) {
    super(message, cause);
  }

  public DeserializationException(Exception cause) {
    super(cause);
  }
}
