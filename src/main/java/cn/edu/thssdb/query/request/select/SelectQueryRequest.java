package cn.edu.thssdb.query.request.select;

import cn.edu.thssdb.query.IQueryRequest;
import cn.edu.thssdb.query.enums.QueryType;
import cn.edu.thssdb.query.request.QueryColumnNameOrValue;
import cn.edu.thssdb.query.request.QueryCondition;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;


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
}
