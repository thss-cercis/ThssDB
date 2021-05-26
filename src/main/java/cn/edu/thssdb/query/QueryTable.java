package cn.edu.thssdb.query;

import cn.edu.thssdb.index.BPlusTreeIterator;
import cn.edu.thssdb.query.enums.QueryConditionComparator;
import cn.edu.thssdb.query.enums.ValueType;
import cn.edu.thssdb.query.request.QueryCondition;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Pair;
import lombok.Getter;
import lombok.var;

import javax.annotation.Nullable;
import java.util.Iterator;

public class QueryTable implements Iterator<Pair<Entry, Row>>, Cloneable {
  @Getter
  private Table table;
  @Nullable
  private QueryCondition condition;

  // iterator and cache
  private Column conditionColumn;
  private int conditionColumnIdx;
  private Comparable conditionValue;
  private BPlusTreeIterator<Entry, Row> iterator;
  private Pair<Entry, Row> cache;

  public QueryTable(Table table, @Nullable QueryCondition condition) {
    // TODO
    this.table = table;
    // TODO: 只有与表相关的 condition 才被设置
    if (condition != null) {
      if (condition.getLhs().getTableName() == null || this.table.tableName.equals(condition.getLhs().getTableName())) {
        for (int i = 0;i < table.columns.length;++i) {
          Column col = table.columns[i];
          if (col.getName().equals(condition.getLhs().getAttrName()) &&
            (
              (condition.getRhs().getValueType() == ValueType.NULL) ||
              (col.getType() == ColumnType.STRING && condition.getRhs().getValueType() == ValueType.STRING) ||
              (col.getType() != ColumnType.STRING && condition.getRhs().getValueType() != ValueType.STRING)
            )
          ) {
            this.condition = condition;
            this.conditionColumnIdx = i;
            this.conditionColumn = col;
            if (condition.getRhs().getValueType() != ValueType.NULL) {
              switch (this.conditionColumn.getType()) {
                case INT:
                  this.conditionValue = Integer.parseInt(condition.getRhs().getValue());
                  break;
                case LONG:
                  this.conditionValue = Long.parseLong(condition.getRhs().getValue());
                  break;
                case FLOAT:
                  this.conditionValue = Float.parseFloat(condition.getRhs().getValue());
                  break;
                case DOUBLE:
                  this.conditionValue = Double.parseDouble(condition.getRhs().getValue());
                  break;
                case STRING:
                  this.conditionValue = condition.getRhs().getValue();
                  break;
                default:
                  break;
              }
            } else {
              this.conditionValue = null;
            }
            break;
          }
        }
      }
    }
    // 设置 cache
    this.iterator = table.index.iterator();
    nextCache();
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    return new QueryTable(table, this.condition);
  }

  @Override
  public boolean hasNext() {
    return cache != null;
  }

  @Override
  public Pair<Entry, Row> next() {
    if (this.cache == null) {
      return null;
    }
    Pair<Entry, Row> ret = this.cache;
    nextCache();
    return ret;
  }

  /**
   * 将 this.cache 设为下一个满足 this.condition 的 {@code Pair<Entry, Row>}
   */
  private void nextCache() {
    this.cache = null;
    while (iterator.hasNext()) {
      var tmp = iterator.next();
      if (this.condition == null) {
        // 不用判断 condition
        this.cache = tmp;
        break;
      }
      // 判断 condition
      boolean isFind = false;
      if (tmp.right.getEntries().get(conditionColumnIdx).value != null) {
        switch (this.condition.getComparator()) {
          case EQ:
            isFind = tmp.right.getEntries().get(conditionColumnIdx).value.compareTo(conditionValue) == 0;
            break;
          case GE:
            isFind = tmp.right.getEntries().get(conditionColumnIdx).value.compareTo(conditionValue) >= 0;
            break;
          case GT:
            isFind = tmp.right.getEntries().get(conditionColumnIdx).value.compareTo(conditionValue) > 0;
            break;
          case LE:
            isFind = tmp.right.getEntries().get(conditionColumnIdx).value.compareTo(conditionValue) <= 0;
            break;
          case LT:
            isFind = tmp.right.getEntries().get(conditionColumnIdx).value.compareTo(conditionValue) < 0;
            break;
          case NE:
            isFind = tmp.right.getEntries().get(conditionColumnIdx).value.compareTo(conditionValue) != 0;
            break;
          default:
            break;
        }
      } else {
        switch (this.condition.getComparator()) {
          case EQ:
            isFind = tmp.right.getEntries().get(conditionColumnIdx).value == null;
            break;
          case NE:
            isFind = tmp.right.getEntries().get(conditionColumnIdx).value != null;
            break;
          default:
            break;
        }
      }
      if (isFind) {
        this.cache = tmp;
        break;
      }
    }
  }
}
