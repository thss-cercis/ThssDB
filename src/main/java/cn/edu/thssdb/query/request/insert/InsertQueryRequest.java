package cn.edu.thssdb.query.request.insert;

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
public class InsertQueryRequest implements IQueryRequest {
  private String tableName;
  /**
   * 如果长度为 0，表示全表.
   */
  private List<String> attrNames = new ArrayList<>();
  /**
   * 存储 insert 语句中指定的 value.
   * 如果 value 为 NULL，则表示为一个 null.
   * 如果 value 为带单引号的字符串，则去除单引号加入.
   */
  private List<String> attrValues = new ArrayList<>();

  @Override
  public QueryType getQueryType() {
    return QueryType.INSERT;
  }
}
