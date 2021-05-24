package cn.edu.thssdb.exception;

public class TableNotExistException extends RuntimeException {
  public TableNotExistException(String message) {
    super(message);
  }

  public TableNotExistException(String message, Exception cause) {
    super(message, cause);
  }
}
