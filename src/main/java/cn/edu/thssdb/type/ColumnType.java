package cn.edu.thssdb.type;

import lombok.var;

import javax.annotation.Nullable;

public enum ColumnType {
  INT("INT"),
  LONG("LONG"),
  FLOAT("FLOAT"),
  DOUBLE("DOUBLE"),
  STRING("STRING");

  public String text;

  ColumnType(String s) {
    this.text = s;
  }

  public static ColumnType fromStr(String s) {
    for (var t: ColumnType.values()) {
      if (t.text.equals(s)) {
        return t;
      }
    }
    return null;
  }

  @Nullable
  public Comparable parse(String s) {
    if (s == null) {
      return null;
    }
    switch (this) {
      case INT:
        return Integer.parseInt(s);
      case LONG:
        return Long.parseLong(s);
      case FLOAT:
        return Float.parseFloat(s);
      case DOUBLE:
        return Double.parseDouble(s);
      case STRING:
      default:
        return s;
    }
  }
}
