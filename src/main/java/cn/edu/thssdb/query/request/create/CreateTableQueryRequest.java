package cn.edu.thssdb.query.request.create;

import cn.edu.thssdb.query.IQueryRequest;
import cn.edu.thssdb.query.enums.QueryType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class CreateTableQueryRequest implements IQueryRequest {
  private String tableName;
  private List<CreateTableAttr> attrs = new ArrayList<>();
  private String primaryKeyName;

  @Override
  public QueryType getQueryType() {
    return QueryType.CREATE_TABLE;
  }
}
