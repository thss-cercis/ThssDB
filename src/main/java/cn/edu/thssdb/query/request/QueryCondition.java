package cn.edu.thssdb.query.request;

import cn.edu.thssdb.query.enums.QueryConditionComparator;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.annotation.Nullable;


@AllArgsConstructor
@NoArgsConstructor
@Data
public class QueryCondition {
  private QueryColumnNameOrValue lhs;
  private QueryConditionComparator comparator;
  private QueryColumnNameOrValue rhs;
}
