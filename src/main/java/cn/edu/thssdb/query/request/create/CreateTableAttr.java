package cn.edu.thssdb.query.request.create;

import cn.edu.thssdb.type.ColumnType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class CreateTableAttr {
  private String attrName;
  private ColumnType columnType;
  /**
   * 当 `columnType == String` 时设置，表示 String 类型的大小
   */
  @Builder.Default
  private Integer stringTypeLen = 0;
  @Builder.Default
  private Boolean isNotNull = false;
}
