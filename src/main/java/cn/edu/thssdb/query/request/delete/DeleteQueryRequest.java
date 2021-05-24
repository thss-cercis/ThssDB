package cn.edu.thssdb.query.request.delete;

import cn.edu.thssdb.query.IQueryRequest;
import cn.edu.thssdb.query.enums.QueryType;
import cn.edu.thssdb.query.request.QueryCondition;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


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
}
