package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

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
  int columnFind(String name) {
    for (int i = 0, s = columns.size(); i < s; ++i) {
      if (columns.get(i).getName().equals(name)) {
        return i;
      }
    }
    return -1;
  }
}
