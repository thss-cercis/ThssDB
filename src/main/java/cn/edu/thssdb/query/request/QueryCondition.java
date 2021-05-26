package cn.edu.thssdb.query.request;

import cn.edu.thssdb.query.enums.QueryConditionComparator;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.annotation.Nonnull;


@AllArgsConstructor
@NoArgsConstructor
@Data
public class QueryCondition {
  @Nonnull
  private QueryColumnNameOrValue lhs;
  private QueryConditionComparator comparator;
  private QueryColumnNameOrValue rhs;

  public void swap() {
    QueryColumnNameOrValue tmp = lhs;
    lhs = rhs;
    rhs = tmp;
  }
}
