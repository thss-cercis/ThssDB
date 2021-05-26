package cn.edu.thssdb.exception;

public class TableInsertException extends RuntimeException {
  public TableInsertException(String message) {
    super(message);
  }

  public TableInsertException(String message, Exception cause) {
    super(message, cause);
  }
}
