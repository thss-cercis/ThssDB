package cn.edu.thssdb.query.request.update;

import cn.edu.thssdb.exception.TableUpdateException;
import cn.edu.thssdb.query.IQueryRequest;
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.query.enums.QueryType;
import cn.edu.thssdb.query.request.QueryColumnNameOrValue;
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
import java.util.stream.IntStream;


@AllArgsConstructor
@NoArgsConstructor
@Data
public class UpdateQueryRequest implements IQueryRequest {
  private String tableName;
  private String columnName;
  /**
   * 一定为 value
   */
  private QueryColumnNameOrValue newValue;
  private QueryCondition condition;

  @Override
  public QueryType getQueryType() {
    return QueryType.UPDATE;
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
      int affectedCount = 0;

      int targetColIdx = IntStream.range(0, table.getColumns().length)
        .filter(i -> table.getColumns()[i].getName().equals(this.columnName))
        .findFirst()
        .orElseThrow(() -> new TableUpdateException("attr name not found in table schema: " + this.columnName));
      Column targetCol = table.getColumns()[targetColIdx];
      if (targetCol.isNotNull() && this.newValue == null) {
        throw new TableUpdateException("could not insert null value into NOT NULL column: " + targetCol.getName());
      }
      Comparable targetNewValue = targetCol.getType().parse(this.newValue.getValue());

      QueryTable qt = new QueryTable(table, this.condition);
      List<Pair<Entry, Row>> pairsToUpdate = new ArrayList<>();
      while (qt.hasNext()) {
        Pair<Entry, Row> p = qt.next();
        // record data to update
        pairsToUpdate.add(p);
        affectedCount += 1;
      }
      // update data
      pairsToUpdate.forEach(p -> {
        List<Pair<Column, Entry>> tmp = new ArrayList<>();
        tmp.add(new Pair<>(targetCol, new Entry(targetNewValue)));
        table.update(p.left, tmp.iterator());
      });

      result.getAttrs().add(
        Cells.fromCell(
          new Cell(String.valueOf(affectedCount))
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
