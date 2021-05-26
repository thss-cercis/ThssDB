package cn.edu.thssdb.query.request.show;

import cn.edu.thssdb.query.IQueryRequest;
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.enums.QueryType;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.utils.Cell;
import cn.edu.thssdb.utils.Cells;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.var;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class ShowTableRequest implements IQueryRequest {
  /**
   * 待展示的表名.
   */
  private String tableName;

  @Override
  public QueryType getQueryType() {
    return QueryType.SHOW_TABLE;
  }

  /**
   * 在数据库上执行操作.
   *
   * @param db 数据库
   * @return 操作结果
   */
  @Override
  public QueryResult execute(Database db) {
    QueryResult result = new QueryResult();
    // 样式
    result.setMetaInfo(MetaInfo.fromColumns(null,
      Column.builder().name("table_name").build(),
      Column.builder().name("attr_name").build(),
      Column.builder().name("attr_type").build(),
      Column.builder().name("attr_constraints").build()
    ));
    Table table = db.getTables().get(this.tableName);
    if (table == null) {
      return result;
    }
    // lock table
    var mutex = table.acquireReadLock();
    mutex.lock();
    try {
      for (Column col : table.getColumns()) {
        result.getAttrs().add(
          Cells.fromCell(
            new Cell(this.tableName),
            new Cell(col.getName()),
            new Cell(col.getType().toString()),
            new Cell((col.getPrimary() > 0 ? "PRIMARY KEY " : "") + (col.isNotNull() ? "NOT NULL " : ""))
          )
        );
      }
    } finally {
      mutex.unlock();
    }
    return result;
  }
}
