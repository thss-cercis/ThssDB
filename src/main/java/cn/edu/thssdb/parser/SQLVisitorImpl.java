package cn.edu.thssdb.parser;

import cn.edu.thssdb.exception.ParseSyntaxException;
import cn.edu.thssdb.exception.UnimplementedOperationException;
import cn.edu.thssdb.query.IQueryRequest;
import cn.edu.thssdb.query.enums.QueryConditionComparator;
import cn.edu.thssdb.query.enums.ValueType;
import cn.edu.thssdb.query.request.QueryColumnNameOrValue;
import cn.edu.thssdb.query.request.QueryCondition;
import cn.edu.thssdb.query.request.create.CreateTableQueryRequest;
import cn.edu.thssdb.query.request.create.CreateTableAttr;
import cn.edu.thssdb.query.request.delete.DeleteQueryRequest;
import cn.edu.thssdb.query.request.drop.DropQueryRequest;
import cn.edu.thssdb.query.request.insert.InsertQueryRequest;
import cn.edu.thssdb.query.request.select.SelectQueryRequest;
import cn.edu.thssdb.query.request.show.ShowMetaQueryRequest;
import cn.edu.thssdb.query.request.show.ShowTablesQueryRequest;
import cn.edu.thssdb.query.request.shutdown.ShutdownQueryRequest;
import cn.edu.thssdb.query.request.transaction.BeginQueryRequest;
import cn.edu.thssdb.query.request.transaction.CommitQueryRequest;
import cn.edu.thssdb.query.request.update.UpdateQueryRequest;
import cn.edu.thssdb.type.ColumnType;
import lombok.var;
import org.antlr.v4.runtime.RuleContext;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SQLVisitorImpl extends SQLBaseVisitor<Object> {
  @Override
  public List<IQueryRequest> visitSql_stmt_list(SQLParser.Sql_stmt_listContext ctx) {
    List<IQueryRequest> ret = new ArrayList<>();
    for (SQLParser.Sql_stmtContext c: ctx.sql_stmt()) {
      ret.add(visitSql_stmt(c));
    }
    return ret;
  }

  @Override
  public IQueryRequest visitSql_stmt(SQLParser.Sql_stmtContext ctx) {
    // * 因为仅仅实现部分 SQL 语句，因此需要用 if else 判断
    if (ctx.create_table_stmt() != null) {
      return visitCreate_table_stmt(ctx.create_table_stmt());
    } else if (ctx.drop_table_stmt() != null) {
      return visitDrop_table_stmt(ctx.drop_table_stmt());
    } else if (ctx.show_table_stmt() != null) {
      return visitShow_table_stmt(ctx.show_table_stmt());
    } else if (ctx.show_meta_stmt() != null) {
      return visitShow_meta_stmt(ctx.show_meta_stmt());
    } else if (ctx.insert_stmt() != null) {
      return visitInsert_stmt(ctx.insert_stmt());
    } else if (ctx.update_stmt() != null) {
      return visitUpdate_stmt(ctx.update_stmt());
    } else if (ctx.select_stmt() != null) {
      return visitSelect_stmt(ctx.select_stmt());
    } else if (ctx.delete_stmt() != null) {
      return visitDelete_stmt(ctx.delete_stmt());
    } else if (ctx.shutdown_stmt() != null) {
      return visitShutdown_stmt(ctx.shutdown_stmt());
    } else if (ctx.begin_stmt() != null) {
      return visitBegin_stmt(ctx.begin_stmt());
    } else if (ctx.commit_stmt() != null) {
      return visitCommit_stmt(ctx.commit_stmt());
    }
    // ! Unimplemented part!
    throw new UnimplementedOperationException("not implemented yet");
  }

  @Override
  public CreateTableQueryRequest visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
    CreateTableQueryRequest req = new CreateTableQueryRequest();
    req.setTableName(visitTable_name(ctx.table_name()));
    // column defs
    req.getAttrs().addAll(ctx.column_def().stream().map(this::visitColumn_def).collect(Collectors.toList()));
    // 检验重复 attr name
    req.getAttrs().forEach(attr -> {
      int count = 0;
      for (CreateTableAttr attrOther: req.getAttrs()) {
        if (attr.getAttrName().equals(attrOther.getAttrName())) {
          count += 1;
        }
      }
      if (count > 1) {
        throw new ParseSyntaxException("duplicate attr name when creating table: " + attr.getAttrName());
      }
    });
    // table constraints
    // TODO: 目前只支持一个属性作为 Primary Key
    if (ctx.table_constraint() == null) {
      throw new ParseSyntaxException("primary key is needed");
    }
    String pkeyName = visitColumn_name(ctx.table_constraint().column_name(0));
    boolean isPKeyInAttrs = req.getAttrs().stream().anyMatch(item -> item.getAttrName().equals(pkeyName));
    if (!isPKeyInAttrs) {
      throw new ParseSyntaxException("primary key is needed");
    }
    req.setPrimaryKeyName(pkeyName);
    var pkCol = req.getAttrs().stream().
      filter(attr -> attr.getAttrName().equals(pkeyName)).
      findFirst().
      orElseThrow(() -> new ParseSyntaxException("could not find primary key"));
    pkCol.setIsNotNull(true);
    return req;
  }

  @Override
  public CreateTableAttr visitColumn_def(SQLParser.Column_defContext ctx) {
    var builder = CreateTableAttr.builder();
    builder.attrName(ctx.column_name().getText());
    String attrTypeStr = ctx.type_name().getText().replaceAll(" ", "");
    if (attrTypeStr.toUpperCase().startsWith(ColumnType.STRING.text)) {
      // String 类型
      builder.columnType(ColumnType.STRING);
      int len = attrTypeStr.length();
      int stringTypeLen = Integer.parseInt(attrTypeStr.substring(7, len - 1));
      if (stringTypeLen <= 0) {
        throw new ParseSyntaxException("length of String type must be greater than 0");
      }
      builder.stringTypeLen(stringTypeLen);
    } else {
      var t = ColumnType.fromStr(attrTypeStr.toUpperCase());
      builder.columnType(t);
    }
    // isNotNull
    if (!ctx.column_constraint().isEmpty()
      && ctx.column_constraint().get(0).K_NOT() != null && ctx.column_constraint().get(0).K_NULL() != null) {
      builder.isNotNull(true);
    }
    return builder.build();
  }

  @Override
  public ShowTablesQueryRequest visitShow_table_stmt(SQLParser.Show_table_stmtContext ctx) {
    return new ShowTablesQueryRequest();
  }

  @Override
  public ShowMetaQueryRequest visitShow_meta_stmt(SQLParser.Show_meta_stmtContext ctx) {
    ShowMetaQueryRequest req = new ShowMetaQueryRequest();
    req.setTableName(visitTable_name(ctx.table_name()));
    return req;
  }

  @Override
  public DropQueryRequest visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
    DropQueryRequest req = new DropQueryRequest();
    // * 忽略 if exists
    req.setTableName(visitTable_name(ctx.table_name()));
    return req;
  }

  @Override
  public InsertQueryRequest visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
    InsertQueryRequest req = new InsertQueryRequest();
    req.setTableName(visitTable_name(ctx.table_name()));
    if (ctx.column_name() != null) {
      req.getAttrNames().addAll(ctx.column_name().stream().map(RuleContext::getText).collect(Collectors.toList()));
    }
    // 检验重复
    Set<String> tmpSet = new HashSet<>(req.getAttrNames());
    if (tmpSet.size() != req.getAttrNames().size()) {
      throw new ParseSyntaxException("duplicate attr name when inserting table");
    }
    req.getAttrValues().addAll(visitValue_entry(ctx.value_entry(0)));
    return req;
  }

  @Override
  public DeleteQueryRequest visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
     DeleteQueryRequest req = new DeleteQueryRequest();
     req.setTableName(visitTable_name(ctx.table_name()));
     req.setCondition(visitCondition(ctx.multiple_condition().condition()));
     return req;
  }

  @Override
  public UpdateQueryRequest visitUpdate_stmt(SQLParser.Update_stmtContext ctx) {
    UpdateQueryRequest req = new UpdateQueryRequest();
    req.setTableName(visitTable_name(ctx.table_name()));
    req.setColumnName(visitColumn_name(ctx.column_name()));
    req.setNewValue(visitComparer(ctx.expression().comparer()));
    if (ctx.multiple_condition() != null) {
      req.setCondition(visitCondition(ctx.multiple_condition().condition()));
    }

    return req;
  }

  @Override
  public SelectQueryRequest visitSelect_stmt(SQLParser.Select_stmtContext ctx) {
    SelectQueryRequest req = new SelectQueryRequest();
    // selectedColumn
    req.getSelectedColumns().addAll(ctx.result_column().stream()
      .map(c -> visitColumn_full_name(c.column_full_name())).collect(Collectors.toList()));
    // from and join
    var tableQueryCtx = ctx.table_query(0);
    req.setTableName(visitTable_name(tableQueryCtx.table_name(0)));
    if (tableQueryCtx.table_name(1) != null) {
      req.setJoinedTableName(visitTable_name(tableQueryCtx.table_name(1)));
      req.setJoinCondition(visitCondition(tableQueryCtx.multiple_condition().condition()));
    }
    // where
    if (ctx.multiple_condition() != null) {
      req.setWhereCondition(visitCondition(ctx.multiple_condition().condition()));
    }
    return req;
  }

  @Override
  public ShutdownQueryRequest visitShutdown_stmt(SQLParser.Shutdown_stmtContext ctx) {
    return new ShutdownQueryRequest();
  }

  @Override
  public BeginQueryRequest visitBegin_stmt(SQLParser.Begin_stmtContext ctx) {
    return new BeginQueryRequest();
  }

  @Override
  public CommitQueryRequest visitCommit_stmt(SQLParser.Commit_stmtContext ctx) {
    return new CommitQueryRequest();
  }

  @Override
  public List<String> visitValue_entry(SQLParser.Value_entryContext ctx) {
    return ctx.literal_value().stream().map(this::visitLiteral_value).collect(Collectors.toList());
  }

  @Override
  public String visitLiteral_value(SQLParser.Literal_valueContext ctx) {
    if (ctx.NUMERIC_LITERAL() != null) {
      return ctx.NUMERIC_LITERAL().getText();
    } else if (ctx.K_NULL() != null) {
      return null;
    } else if (ctx.STRING_LITERAL() != null) {
      int len = ctx.STRING_LITERAL().getText().length();
      return ctx.STRING_LITERAL().getText().substring(1, len - 1);
    } else {
      return "";
    }
  }

  @Override
  public QueryCondition visitCondition(SQLParser.ConditionContext ctx) {
    QueryCondition cond = new QueryCondition();
    cond.setComparator(visitComparator(ctx.comparator()));

    var lhs = visitColumn_full_name(ctx.expression(0).comparer().column_full_name());
    cond.setLhs(lhs);

    cond.setRhs(visitComparer(ctx.expression(1).comparer()));

    return cond;
  }

  @Override public QueryConditionComparator visitComparator(SQLParser.ComparatorContext ctx) {
    return QueryConditionComparator.fromSymbol(ctx.getText());
  }

  @Override public QueryColumnNameOrValue visitComparer(SQLParser.ComparerContext ctx) {
    if (ctx.column_full_name() != null) {
      return visitColumn_full_name(ctx.column_full_name());
    } else {
      if (ctx.literal_value().NUMERIC_LITERAL() != null) {
        return new QueryColumnNameOrValue(null, null, ValueType.NUMERIC, visitLiteral_value(ctx.literal_value()));
      } else if (ctx.literal_value().K_NULL() != null) {
        return new QueryColumnNameOrValue(null, null, ValueType.NULL, visitLiteral_value(ctx.literal_value()));
      } else {
        return new QueryColumnNameOrValue(null, null, ValueType.STRING, visitLiteral_value(ctx.literal_value()));
      }
    }
  }

  @Override
  public QueryColumnNameOrValue visitColumn_full_name(SQLParser.Column_full_nameContext ctx) {
    var builder = QueryColumnNameOrValue.builder();
    if (ctx.table_name() != null) {
      builder.tableName(visitTable_name(ctx.table_name()));
    }
    builder.attrName(visitColumn_name(ctx.column_name()));
    return builder.build();
  }

  @Override
  public String visitColumn_name(SQLParser.Column_nameContext ctx) {
    return ctx.IDENTIFIER().getText();
  }

  @Override
  public String visitTable_name(SQLParser.Table_nameContext ctx) {
    return ctx.IDENTIFIER().getText();
  }
}
