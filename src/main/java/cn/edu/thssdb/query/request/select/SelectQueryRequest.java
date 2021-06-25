package cn.edu.thssdb.query.request.select;

import cn.edu.thssdb.exception.TableInsertException;
import cn.edu.thssdb.exception.TableSelectException;
import cn.edu.thssdb.query.IQueryRequest;
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.query.enums.QueryType;
import cn.edu.thssdb.query.request.QueryColumnNameOrValue;
import cn.edu.thssdb.query.request.QueryCondition;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;
import lombok.*;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


@AllArgsConstructor
@NoArgsConstructor
@Data
public class SelectQueryRequest implements IQueryRequest {
  private List<QueryColumnNameOrValue> selectedColumns = new ArrayList<>();
  private String tableName;
  /**
   * join 相关的两个变量
   */
  @Nullable private String joinedTableName;
  @Nullable private QueryCondition joinCondition;
  /**
   * where 的条件
   */
  @Nullable private QueryCondition whereCondition;

  @Override
  public QueryType getQueryType() {
    return QueryType.SELECT;
  }

  /**
   * 在数据库上执行操作.
   *
   * @param db 数据库
   * @return 操作结果
   */
  @Override
  public QueryResult execute(Database db) {
    db.getDbLock().readLock().lock();
    QueryResult result;
    // 样式
    // logic
    Table tb1 = db.getTables().get(this.tableName);
    if (tb1 == null) {
      throw new TableSelectException("could not find table: " + tableName);
    }
    // lock table
    var mutex = tb1.acquireReadLock();
    mutex.lock();
    try {
      if (this.joinedTableName != null && this.joinCondition != null) {
        // join table select
        // first, find the table
        Table tb2 = db.getTables().get(this.joinedTableName);
        if (tb2 == null) {
          throw new TableSelectException("could not find table: " + this.joinedTableName);
        }
        // lock the join table
        var mutex2 = tb2.acquireReadLock();
        mutex2.lock();
        try {
          // set meta info
          MetaInfo metaInfo = MetaInfo.fromColumns(null,
            this.selectedColumns.stream()
              .map(q -> new Column(q.getAttrName(), ColumnType.STRING, 0, false, 0))
              .collect(Collectors.toList())
          );
          metaInfo.setTableNames(new ArrayList<>());
          this.selectedColumns.forEach(q -> {
            assert metaInfo.getTableNames() != null;
            metaInfo.getTableNames().add(q.getTableName());
          });
          // get table
          QueryTable qt1 = new QueryTable(tb1, this.whereCondition);
          QueryTable qt2 = new QueryTable(tb2, this.whereCondition);
          result = QueryResult.fromJoinTable(metaInfo, qt1, qt2, this.joinCondition);
        } finally {
          mutex2.unlock();
        }
      } else {
        // normal single select
        MetaInfo metaInfo = MetaInfo.fromColumns(null,
          this.selectedColumns.stream()
            .map(q -> new Column(q.getAttrName(), ColumnType.STRING, 0, false, 0))
            .collect(Collectors.toList())
        );
        QueryTable qt = new QueryTable(tb1, this.whereCondition);
        result = QueryResult.fromSingleTable(metaInfo, qt);
      }
    } finally {
      mutex.unlock();
      db.getDbLock().readLock().unlock();
    }

    return result;
//    return null;
  }
}
