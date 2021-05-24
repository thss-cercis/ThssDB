package cn.edu.thssdb.query.enums;

import lombok.var;

import javax.annotation.Nullable;

public enum CreateTableColumnType {
  INT("INT"),
  LONG("LONG"),
  FLOAT("FLOAT"),
  DOUBLE("DOUBLE"),
  STRING("STRING");

  public String text;

  CreateTableColumnType(String s) {
    this.text = s;
  }

  public static CreateTableColumnType fromStr(String s) {
    for (var t: CreateTableColumnType.values()) {
      if (t.text.equals(s)) {
        return t;
      }
    }
    return null;
  }
}
