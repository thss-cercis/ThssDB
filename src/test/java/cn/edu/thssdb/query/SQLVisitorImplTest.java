package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLVisitor;
import cn.edu.thssdb.parser.SQLVisitorImpl;
import cn.edu.thssdb.query.request.create.CreateTableQueryRequest;
import cn.edu.thssdb.query.request.drop.DropQueryRequest;
import cn.edu.thssdb.query.request.insert.InsertQueryRequest;
import cn.edu.thssdb.query.request.select.SelectQueryRequest;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class SQLVisitorImplTest {

  @Before
  public void setUp() {

  }

  @Test
  public void testDropTable() {
    String input = "DROP TABLE hello; drop table world";
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(input));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    SQLVisitor visitor = new SQLVisitorImpl();

    List<DropQueryRequest> request = (List<DropQueryRequest>) visitor.visit(parser.parse());
    assertEquals(2, request.size());
    assertEquals("world", request.get(1).getTableName());
  }

  @Test
  public void testCreateTable() {
    String input = "CREATE TABLE suck(a int, b double, s String( 10 ), PRIMARY KEY(a));";
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(input));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    SQLVisitor visitor = new SQLVisitorImpl();

    CreateTableQueryRequest request = ((List<CreateTableQueryRequest>) visitor.visit(parser.parse())).get(0);
    assertEquals(3, request.getAttrs().size());
    assertEquals(10, request.getAttrs().get(2).getStringTypeLen().intValue());
  }

  @Test
  public void testInsertTable1() {
    String input = "INSERT INTO t(a,b,c) VALUES (1, 2, '123');";
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(input));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    SQLVisitor visitor = new SQLVisitorImpl();

    InsertQueryRequest request = ((List<InsertQueryRequest>) visitor.visit(parser.parse())).get(0);
    assertEquals(3, request.getAttrNames().size());
    assertEquals("123", request.getAttrValues().get(2));
  }

  @Test
  public void testInsertTable2() {
    String input = "INSERT INTO t VALUES (1, 2, '123');";
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(input));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    SQLVisitor visitor = new SQLVisitorImpl();

    InsertQueryRequest request = ((List<InsertQueryRequest>) visitor.visit(parser.parse())).get(0);
    assertEquals(0, request.getAttrNames().size());
    assertEquals("123", request.getAttrValues().get(2));
  }

  @Test
  public void testSelectTable1() {
    String input = "SELECT a FROM t WHERE c = '123'";
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(input));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    SQLVisitor visitor = new SQLVisitorImpl();

    SelectQueryRequest request = ((List<SelectQueryRequest>) visitor.visit(parser.parse())).get(0);
    assertEquals(1, request.getSelectedColumns().size());
    assertEquals("123", request.getWhereCondition().getRhs().getValue());
  }

  @Test
  public void testSelectTable2() {
    String input = "SELECT a.z FROM t WHERE a.c = '123'";
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(input));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    SQLVisitor visitor = new SQLVisitorImpl();

    SelectQueryRequest request = ((List<SelectQueryRequest>) visitor.visit(parser.parse())).get(0);
    assertEquals(1, request.getSelectedColumns().size());
    assertEquals("c", request.getWhereCondition().getLhs().getAttrName());
  }
}
