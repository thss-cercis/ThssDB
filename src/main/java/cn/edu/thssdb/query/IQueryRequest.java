package cn.edu.thssdb.query;

import cn.edu.thssdb.query.enums.QueryType;
import cn.edu.thssdb.schema.Database;

public interface IQueryRequest {
  /**
   * 获取请求的类型.
   * @return 请求类型
   */
  QueryType getQueryType();

  /**
   * 在数据库上执行操作.
   * @param db 数据库
   * @return 操作结果
   */
  QueryResult execute(Database db);
}
