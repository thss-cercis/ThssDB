package cn.edu.thssdb.query.request.insert;

import cn.edu.thssdb.exception.TableInsertException;
import cn.edu.thssdb.query.IQueryRequest;
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.enums.QueryType;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.utils.Cell;
import cn.edu.thssdb.utils.Cells;
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
public class InsertQueryRequest implements IQueryRequest {
  private String tableName;
  /**
   * 如果长度为 0，表示全表.
   */
  private List<String> attrNames = new ArrayList<>();
  /**
   * 存储 insert 语句中指定的 value.
   * 如果 value 为 NULL，则表示为一个 null.
   * 如果 value 为带单引号的字符串，则去除单引号加入.
   */
  private List<String> attrValues = new ArrayList<>();

  @Override
  public QueryType getQueryType() {
    return QueryType.INSERT;
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
      return result;
    }
    // lock table
    var mutex = table.acquireWriteLock();
    mutex.lock();
    try {
      Entry[] entriesToInsert = new Entry[table.getColumns().length];
      if (attrNames.isEmpty()) {
        // 全表插入
        if (attrValues.size() != table.getColumns().length) {
          // 属性数量不对
          throw new TableInsertException("length of input do not match the length of attrs");
        }
        for (int i = 0;i < attrValues.size();++i) {
          String valueStr = attrValues.get(i);
          Column col = table.getColumns()[i];
          if (col.isNotNull() && valueStr == null) {
            throw new TableInsertException("could not insert null value into NOT NULL column: " + col.getName());
          }
          entriesToInsert[i] = new Entry(col.getType().parse(valueStr));
        }
      } else {
        // 部分属性插入，需要检测 not null
        for (int i = 0;i < attrNames.size();++i) {
          String attrName = attrNames.get(i);
          String valueStr = attrValues.get(i);
          int attrColIdx = IntStream.range(0, table.getColumns().length)
            .filter(idx -> table.getColumns()[idx].getName().equals(attrName))
            .findFirst()
            .orElseThrow(() -> new TableInsertException("attr " + attrName + " not found in table " + tableName));
          Column attrCol = table.getColumns()[attrColIdx];
          if (attrCol.isNotNull() && valueStr == null) {
            throw new TableInsertException("could not insert null value into NOT NULL column: " + attrCol.getName());
          }
          entriesToInsert[attrColIdx] = new Entry(attrCol.getType().parse(valueStr));
        }
        // 检验 not null
        for (int i = 0;i < table.getColumns().length;++i) {
          Column col = table.getColumns()[i];
          if (col.isNotNull() && entriesToInsert[i] == null) {
            throw new TableInsertException("could not insert null value into NOT NULL column: " + col.getName());
          }
        }
      }
      table.insert(entriesToInsert);
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
    } finally {
      mutex.unlock();
    }

    return result;
  }
}
