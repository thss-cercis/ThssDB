package cn.edu.thssdb.exception;

public class KeyNotExistException extends RuntimeException {
  public KeyNotExistException(String message) {
    super(message);
  }

  public KeyNotExistException() {
    super("Exception: key doesn't exist!");
  }
}
