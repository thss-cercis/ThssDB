package cn.edu.thssdb.query.request.show;

import cn.edu.thssdb.query.IQueryRequest;
import cn.edu.thssdb.query.enums.QueryType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class ShowTableRequest implements IQueryRequest {
  /**
   * 待展示的表名.
   */
  private String tableName;

  @Override
  public QueryType getQueryType() {
    return QueryType.SHOW_TABLE;
  }
}
