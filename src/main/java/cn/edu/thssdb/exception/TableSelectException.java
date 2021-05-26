package cn.edu.thssdb.exception;

public class TableSelectException extends RuntimeException {
  public TableSelectException(String message) {
    super(message);
  }

  public TableSelectException(String message, Exception cause) {
    super(message, cause);
  }
}
