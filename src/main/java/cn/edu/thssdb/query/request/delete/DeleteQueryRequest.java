package cn.edu.thssdb.query.request.delete;

import cn.edu.thssdb.query.IQueryRequest;
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.query.enums.QueryType;
import cn.edu.thssdb.query.request.QueryCondition;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.utils.Cell;
import cn.edu.thssdb.utils.Cells;
import cn.edu.thssdb.utils.Pair;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.var;

import java.util.ArrayList;
import java.util.List;


@AllArgsConstructor
@NoArgsConstructor
@Data
public class DeleteQueryRequest implements IQueryRequest {
  private String tableName;
  private QueryCondition condition;

  @Override
  public QueryType getQueryType() {
    return QueryType.DELETE;
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
    // logic
    Table table = db.getTables().get(this.tableName);
    if (table == null) {
      result.getAttrs().add(
        Cells.fromCell(
          new Cell("0")
        )
      );
      return result;
    }
    // lock table
    var mutex = table.acquireWriteLock();
    mutex.lock();
    try {
      List<Entry> entriesToDelete = new ArrayList<>();
      int pkIdx = table.getPrimaryIndex();
      QueryTable qt = new QueryTable(table, this.condition);
      while (qt.hasNext()) {
        Pair<Entry, Row> r = qt.next();
        entriesToDelete.add(r.right.getEntries().get(pkIdx));
      }
      entriesToDelete.forEach(table::delete);
      result.getAttrs().add(
        Cells.fromCell(
          new Cell(String.valueOf(entriesToDelete.size()))
        )
      );
    } catch (Exception e) {
      result.getAttrs().add(
        Cells.fromCell(
          new Cell(e.getMessage())
        )
      );
    } finally {
      mutex.unlock();
    }

    return result;
  }
}
