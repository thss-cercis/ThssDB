package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.utils.Cell;
import lombok.Data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

@Data
public class QueryResult {

  private final List<MetaInfo> metaInfoInfos;
  private final List<Integer> index;
  private final List<Cell> attrs;

  public QueryResult(QueryTable[] queryTables) {
    // TODO
    this.metaInfoInfos = new ArrayList<>();
    this.index = new ArrayList<>();
    this.attrs = new ArrayList<>();
  }

  public static Row combineRow(LinkedList<Row> rows) {
    // TODO
    return null;
  }

  public Row generateQueryRecord(Row row) {
    // TODO
    return null;
  }
}
