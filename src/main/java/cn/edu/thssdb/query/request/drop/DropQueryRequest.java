package cn.edu.thssdb.query.request.drop;

import cn.edu.thssdb.query.IQueryRequest;
import cn.edu.thssdb.query.enums.QueryType;
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
}
