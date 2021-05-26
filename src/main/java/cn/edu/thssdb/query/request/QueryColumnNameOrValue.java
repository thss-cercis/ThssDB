package cn.edu.thssdb.query.request;

import cn.edu.thssdb.query.enums.ValueType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.annotation.Nullable;


@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class QueryColumnNameOrValue {
  @Nullable
  private String tableName;
  private String attrName;
  private ValueType valueType;
  private String value;
}
