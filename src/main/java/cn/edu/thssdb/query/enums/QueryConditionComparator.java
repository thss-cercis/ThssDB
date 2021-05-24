package cn.edu.thssdb.query.enums;

import lombok.var;

public enum QueryConditionComparator {
//  AND,
//  OR,
  GT(">"),
  GE(">="),
  LT("<"),
  LE("<="),
  NE("<>"),
  EQ("=");

  public String symbol;
  QueryConditionComparator(String symbol) {
    this.symbol = symbol;
  }

  public static QueryConditionComparator fromSymbol(String s) {
    for (var v: QueryConditionComparator.values()) {
      if (v.symbol.equals(s)) {
        return v;
      }
    }
    return null;
  }
}
