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
import lombok.var;

public class ShowTablesQueryRequest implements IQueryRequest {
  /**
   * 获取请求的类型.
   *
   * @return 请求类型
   */
  @Override
  public QueryType getQueryType() {
    return QueryType.SHOW_TABLES;
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
      Column.builder().name("table_name").build()
    ));
    // lock db
    var mutex = db.getDbLock().readLock();
    mutex.lock();
    try {
      var tables = db.getTables();
      for (Table table: tables.values()) {
        result.getAttrs().add(
          Cells.fromCell(
            new Cell(table.getTableName())
          )
        );
      }
    } finally {
      mutex.unlock();
    }

    return result;
  }
}
