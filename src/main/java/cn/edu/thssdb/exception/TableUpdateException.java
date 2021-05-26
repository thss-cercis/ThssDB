package cn.edu.thssdb.exception;

public class TableUpdateException extends RuntimeException {
  public TableUpdateException(String message) {
    super(message);
  }

  public TableUpdateException(String message, Exception cause) {
    super(message, cause);
  }
}
