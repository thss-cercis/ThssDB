package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

@AllArgsConstructor
@NoArgsConstructor
@Data
public
class MetaInfo {
  @Nullable
  private List<String> tableNames;
  private final List<Column> columns = new ArrayList<>();

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

  public static MetaInfo fromColumns(@Nullable String tableName, Column... cols) {
    MetaInfo ret = new MetaInfo();
    if (tableName != null) {
      ret.tableNames = new ArrayList<>();
      for (int i = 0;i < cols.length;++i) {
        ret.tableNames.add(tableName);
      }
    }
    ret.columns.addAll(Arrays.asList(cols));
    return ret;
  }

  public static MetaInfo fromColumns(@Nullable String tableName, List<Column> cols) {
    MetaInfo ret = new MetaInfo();
    if (tableName != null) {
      ret.tableNames = new ArrayList<>();
      for (int i = 0;i < cols.size();++i) {
        ret.tableNames.add(tableName);
      }
    }
    ret.columns.addAll(cols);
    return ret;
  }
}
