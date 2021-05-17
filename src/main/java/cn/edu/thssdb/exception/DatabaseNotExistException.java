package cn.edu.thssdb.exception;

public class DatabaseNotExistException extends RuntimeException {
  public DatabaseNotExistException(String message) {
    super(message);
  }

  public DatabaseNotExistException(String message, Exception cause) {
    super(message, cause);
  }

  public DatabaseNotExistException(Exception cause) {
    super(cause);
  }
}
