package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

@Data
class MetaInfo {

  private final String tableName;
  private final List<Column> columns;

  MetaInfo(String tableName, ArrayList<Column> columns) {
    this.tableName = tableName;
    this.columns = columns;
  }

  /**
   * Finds a column in a table. Returns the column index, or -1 if not found.
   *
   * @param name column name
   * @return index of the column, or -1 if not found
   */
  int findColumnIndex(String name) {
    return IntStream.range(0, columns.size()).
      filter(i -> columns.get(i).getName().equals(name)).
      findFirst().
      orElse(-1);
  }
}
