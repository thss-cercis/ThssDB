package cn.edu.thssdb.query.request.drop;

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

@AllArgsConstructor
@NoArgsConstructor
@Data
public class DropQueryRequest implements IQueryRequest {
  /**
   * 待删除的表名.
   */
  private String tableName;

  @Override
  public QueryType getQueryType() {
    return QueryType.DROP_TABLE;
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
      Column.builder().name("affected").build()
    ));
    Table table = db.getTables().get(this.tableName);
    if (table == null) {
      result.getAttrs().add(
        Cells.fromCell(
          new Cell("0")
        )
      );
      return result;
    }
    try {
      db.dropTable(this.tableName);
      result.getAttrs().add(
        Cells.fromCell(
          new Cell("1")
        )
      );
    } catch (Exception e) {
      result.getAttrs().add(
        Cells.fromCell(
          new Cell(e.getMessage())
        )
      );
    }

    return result;
  }
}
