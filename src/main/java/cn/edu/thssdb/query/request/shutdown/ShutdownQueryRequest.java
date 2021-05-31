package cn.edu.thssdb.query.request.shutdown;

import cn.edu.thssdb.exception.DBMSShutdownException;
import cn.edu.thssdb.exception.UnimplementedOperationException;
import cn.edu.thssdb.query.IQueryRequest;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.enums.QueryType;
import cn.edu.thssdb.schema.Database;

public class ShutdownQueryRequest implements IQueryRequest {
  /**
   * 获取请求的类型.
   *
   * @return 请求类型
   */
  @Override
  public QueryType getQueryType() {
    return QueryType.SHUTDOWN;
  }

  /**
   * 在数据库上执行操作.
   *
   * @param db 数据库
   * @return 操作结果
   */
  @Override
  public QueryResult execute(Database db) {
    throw new DBMSShutdownException();
  }
}
