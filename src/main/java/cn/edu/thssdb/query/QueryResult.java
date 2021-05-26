package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.TableSelectException;
import cn.edu.thssdb.query.enums.QueryConditionComparator;
import cn.edu.thssdb.query.request.QueryCondition;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.utils.Cell;
import cn.edu.thssdb.utils.Cells;
import cn.edu.thssdb.utils.Pair;
import lombok.*;

import java.util.*;
import java.util.stream.IntStream;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class QueryResult {
  private MetaInfo metaInfo;
  private final List<Cells> attrs = new ArrayList<>();

  public static QueryResult fromSingleTable(MetaInfo metaInfo, QueryTable t) {
    Column[] columns = t.getTable().columns;
    // 构建 columns 到 metaInfo 的索引映射
    Integer[] mapping = new Integer[metaInfo.getColumns().size()];
    for (int i = 0;i < metaInfo.getColumns().size();++i) {
      Column metaInfoCol = metaInfo.getColumns().get(i);
      mapping[i] = IntStream.range(0, columns.length)
        .filter(idx -> metaInfoCol.getName().equals(columns[idx].getName()))
        .findFirst()
        .orElseThrow(() -> new TableSelectException("could not find attr name in table"));
    }
    // 检验重复
    Set<Integer> tmpSet = new HashSet<>(Arrays.asList(mapping));
    if (tmpSet.size() != mapping.length) {
      throw new TableSelectException("duplicate selected attr");
    }
    // 加入数据
    QueryResult ret = new QueryResult();
    ret.setMetaInfo(metaInfo);
    while (t.hasNext()) {
      Pair<Entry, Row> p = t.next();
      Cells newCells = new Cells();
      for (int i = 0;i < metaInfo.getColumns().size();++i) {
        int targetEntryIdx = mapping[i];
        newCells.getCells().add(
          new Cell(p.right.getEntries().get(targetEntryIdx).value.toString())
        );
      }
      ret.getAttrs().add(newCells);
    }

    return ret;
  }

  @SneakyThrows
  public static QueryResult fromJoinTable(MetaInfo metaInfo, QueryTable t1, QueryTable t2, QueryCondition joinCondition) {
    // 检验 join condition 正确性
    assert joinCondition != null;
    assert joinCondition.getComparator() == QueryConditionComparator.EQ;
    assert joinCondition.getLhs().getAttrName() != null;
    assert joinCondition.getRhs().getAttrName() != null;
    if (!t1.getTable().getTableName().equals(joinCondition.getLhs().getTableName())) {
      joinCondition.swap();
    }
    if (!t1.getTable().getTableName().equals(joinCondition.getLhs().getTableName())) {
      throw new TableSelectException("table name of join operators must be specified correctly");
    }
    // main process
    Column[] columns1 = t1.getTable().columns;
    Column[] columns2 = t2.getTable().columns;
    // 构建 columns 到 metaInfo 的索引映射
    Integer[] mapping = new Integer[metaInfo.getColumns().size()];
    for (int i = 0;i < metaInfo.getColumns().size();++i) {
      assert metaInfo.getTableNames() != null;
      String metaInfoTableName = metaInfo.getTableNames().get(i);
      Column metaInfoCol = metaInfo.getColumns().get(i);
      if (metaInfoTableName == null || metaInfoTableName.equals(t1.getTable().getTableName())) {
        // 从 tb1 选择
        mapping[i] = IntStream.range(0, columns1.length)
          .filter(idx -> metaInfoCol.getName().equals(columns1[idx].getName()))
          .findFirst()
          .orElseThrow(() -> new TableSelectException("could not find attr name in table: " + t1.getTable().getTableName()));
      } else if (metaInfoTableName.equals(t2.getTable().getTableName())) {
        // 从 tb2 选择
        mapping[i] = IntStream.range(0, columns2.length)
          .filter(idx -> metaInfoCol.getName().equals(columns2[idx].getName()))
          .findFirst()
          .orElseThrow(() -> new TableSelectException("could not find attr name in table: " + t2.getTable().getTableName()));
      } else {
        throw new TableSelectException("table doesn't exists: " + metaInfoTableName);
      }
    }
    // TODO: 难以检验重复，懒惰了
    // 找到 join 条件中，在两个表中各自的 entry index
    int targetEntryIdx1 = IntStream.range(0, columns1.length)
      .filter(i -> columns1[i].getName().equals(joinCondition.getLhs().getAttrName()))
      .findFirst()
      .orElseThrow(() -> new TableSelectException("could not find attr " + joinCondition.getLhs().getAttrName() + " in table " + t1.getTable().getTableName()));
    int targetEntryIdx2 = IntStream.range(0, columns2.length)
      .filter(i -> columns2[i].getName().equals(joinCondition.getLhs().getAttrName()))
      .findFirst()
      .orElseThrow(() -> new TableSelectException("could not find attr " + joinCondition.getLhs().getAttrName() + " in table " + t2.getTable().getTableName()));
    // 加入数据
    QueryResult ret = new QueryResult();
    ret.setMetaInfo(metaInfo);
    while (t1.hasNext()) {
      Pair<Entry, Row> p1 = t1.next();
      Comparable targetValue1 = p1.right.getEntries().get(targetEntryIdx1).value;
      QueryTable t2Cloned = (QueryTable) t2.clone();
      while (t2Cloned.hasNext()) {
        Pair<Entry, Row> p2 = t2Cloned.next();
        Comparable targetValue2 = p2.right.getEntries().get(targetEntryIdx2).value;
        if (targetValue1.compareTo(targetValue2) != 0) {
          continue;
        }
        Cells newCells = new Cells();
        for (int i = 0;i < metaInfo.getColumns().size();++i) {
          int tIdx = mapping[i];
          String metaInfoTableName = metaInfo.getTableNames().get(i);
          if (metaInfoTableName == null || metaInfoTableName.equals(t1.getTable().getTableName())) {
            // tb1
            newCells.getCells().add(
              new Cell(p1.right.getEntries().get(tIdx).value.toString())
            );
          } else {
            // tb2
            newCells.getCells().add(
              new Cell(p2.right.getEntries().get(tIdx).value.toString())
            );
          }
        }
        ret.getAttrs().add(newCells);
      }
    }

    return ret;
  }
}
