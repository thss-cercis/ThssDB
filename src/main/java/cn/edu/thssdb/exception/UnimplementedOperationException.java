package cn.edu.thssdb.exception;

public class UnimplementedOperationException extends RuntimeException {
  public UnimplementedOperationException(String message) {
    super(message);
  }

  public UnimplementedOperationException(String message, Exception cause) {
    super(message, cause);
  }
}
