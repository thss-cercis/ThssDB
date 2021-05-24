package cn.edu.thssdb.query.request.create;

import cn.edu.thssdb.query.enums.CreateTableColumnType;
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
  private CreateTableColumnType columnType;
  /**
   * 当 `columnType == String` 时设置，表示 String 类型的大小
   */
  private Integer stringTypeLen = 0;
  private Boolean isNotNull = false;
}
