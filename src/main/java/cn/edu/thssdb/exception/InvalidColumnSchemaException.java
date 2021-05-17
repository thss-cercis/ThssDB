package cn.edu.thssdb.exception;

public class InvalidColumnSchemaException extends RuntimeException {
  public InvalidColumnSchemaException(String message) {
    super(message);
  }

  public InvalidColumnSchemaException(String message, Exception cause) {
    super(message, cause);
  }
}
