package cn.edu.thssdb.query.request.update;

import cn.edu.thssdb.query.IQueryRequest;
import cn.edu.thssdb.query.enums.QueryType;
import cn.edu.thssdb.query.request.QueryColumnNameOrValue;
import cn.edu.thssdb.query.request.QueryCondition;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


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
}
