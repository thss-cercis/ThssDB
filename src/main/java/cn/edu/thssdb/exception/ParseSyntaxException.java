package cn.edu.thssdb.exception;

public class ParseSyntaxException extends RuntimeException {
  public ParseSyntaxException(String message) {
    super(message);
  }

  public ParseSyntaxException(String message, Exception cause) {
    super(message, cause);
  }
}
