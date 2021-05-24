package cn.edu.thssdb.exception;

public class TableAlreadyExistException extends RuntimeException {
  public TableAlreadyExistException(String message) {
    super(message);
  }

  public TableAlreadyExistException(String message, Exception cause) {
    super(message, cause);
  }
}
