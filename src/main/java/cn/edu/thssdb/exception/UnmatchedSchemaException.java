package cn.edu.thssdb.exception;

import java.io.IOException;

public class UnmatchedSchemaException extends IOException {
  public UnmatchedSchemaException(String message) {
    super(message);
  }

  public UnmatchedSchemaException() {
    super("Schema not matched");
  }
}
