package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLVisitor;
import cn.edu.thssdb.parser.SQLVisitorImpl;
import cn.edu.thssdb.query.request.create.CreateTableQueryRequest;
import cn.edu.thssdb.query.request.delete.DeleteQueryRequest;
import cn.edu.thssdb.query.request.drop.DropQueryRequest;
import cn.edu.thssdb.query.request.insert.InsertQueryRequest;
import cn.edu.thssdb.query.request.select.SelectQueryRequest;
import cn.edu.thssdb.query.request.show.ShowTableRequest;
import cn.edu.thssdb.query.request.update.UpdateQueryRequest;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import lombok.var;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class QueryRequestTest {
  @Before
  public void setUp() {

  }

  @Test
  public void test01() {
    Database db = Manager.getInstance().getDatabases().get("thss");

    String input = "CREATE TABLE hello(a INT, b DOUBLE, c STRING(8), PRIMARY KEY(a));";
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(input));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    SQLVisitor visitor = new SQLVisitorImpl();
    CreateTableQueryRequest request = ((List<CreateTableQueryRequest>) visitor.visit(parser.parse())).get(0);

    var result = request.execute(db);
    assertEquals("affected", result.getMetaInfo().getColumns().get(0).getName());
    assertEquals(1, result.getAttrs().size());
    assertEquals("1", result.getAttrs().get(0).getCells().get(0).getValue());
  }

  @Test
  public void test02() {
    Database db = Manager.getInstance().getDatabases().get("thss");

    String input = "INSERT INTO hello VALUES (1, 2.3, 'nihao');INSERT INTO hello VALUES (10, 19.8, 'cnm');";
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(input));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    SQLVisitor visitor = new SQLVisitorImpl();
    var tmp = ((List<InsertQueryRequest>) visitor.visit(parser.parse()));
    InsertQueryRequest request1 = tmp.get(0);
    InsertQueryRequest request2 = tmp.get(1);

    var result = request1.execute(db);
    assertEquals(1, db.getTables().get("hello").index.size());

    result = request2.execute(db);
    assertEquals(2, db.getTables().get("hello").index.size());
  }

  @Test
  public void test03() {
    Database db = Manager.getInstance().getDatabases().get("thss");

    String input = "UPDATE hello SET b = 3.2 WHERE a = 1;";
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(input));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    SQLVisitor visitor = new SQLVisitorImpl();
    UpdateQueryRequest request = ((List<UpdateQueryRequest>) visitor.visit(parser.parse())).get(0);

    var result = request.execute(db);
    assertEquals("1", result.getAttrs().get(0).getCells().get(0).getValue());
  }

  @Test
  public void test04() {
    Database db = Manager.getInstance().getDatabases().get("thss");

    String input = "SHOW TABLE hello;";
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(input));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    SQLVisitor visitor = new SQLVisitorImpl();
    ShowTableRequest request = ((List<ShowTableRequest>) visitor.visit(parser.parse())).get(0);

    var result = request.execute(db);
    assertEquals("hello", result.getAttrs().get(0).getCells().get(0).getValue());
  }

  @Test
  public void test05() {
    Database db = Manager.getInstance().getDatabases().get("thss");

    String input = "SELECT c FROM hello;";
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(input));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    SQLVisitor visitor = new SQLVisitorImpl();
    SelectQueryRequest request = ((List<SelectQueryRequest>) visitor.visit(parser.parse())).get(0);

    var result = request.execute(db);
    assertEquals(2, result.getAttrs().size());
  }

  @Test
  public void test06() {
    Database db = Manager.getInstance().getDatabases().get("thss");

    String input = "SELECT a,c FROM hello WHERE b = 19.8;";
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(input));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    SQLVisitor visitor = new SQLVisitorImpl();
    SelectQueryRequest request = ((List<SelectQueryRequest>) visitor.visit(parser.parse())).get(0);

    var result = request.execute(db);
    assertEquals(1, result.getAttrs().size());
  }

  @Test
  public void test21() {
    Database db = Manager.getInstance().getDatabases().get("thss");

    String input = "CREATE TABLE world(a INT, d FLOAT, e STRING(16), PRIMARY KEY(a));";
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(input));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    SQLVisitor visitor = new SQLVisitorImpl();
    CreateTableQueryRequest request = ((List<CreateTableQueryRequest>) visitor.visit(parser.parse())).get(0);

    var result = request.execute(db);
    assertEquals("affected", result.getMetaInfo().getColumns().get(0).getName());
    assertEquals(1, result.getAttrs().size());
    assertEquals("1", result.getAttrs().get(0).getCells().get(0).getValue());
  }

  @Test
  public void test22() {
    Database db = Manager.getInstance().getDatabases().get("thss");

    String input = "INSERT INTO world VALUES (1, 114514, 'bonjour'); INSERT INTO world VALUES (10, 9.8, 'osu');";
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(input));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    SQLVisitor visitor = new SQLVisitorImpl();
    var tmp = ((List<InsertQueryRequest>) visitor.visit(parser.parse()));
    InsertQueryRequest request1 = tmp.get(0);
    InsertQueryRequest request2 = tmp.get(1);

    var result = request1.execute(db);
    assertEquals(1, db.getTables().get("world").index.size());

    result = request2.execute(db);
    assertEquals(2, db.getTables().get("world").index.size());
  }

  @Test
  public void test23() {
    Database db = Manager.getInstance().getDatabases().get("thss");

    String input = "SELECT hello.a, world.e FROM hello JOIN world ON hello.a = world.a;";
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(input));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    SQLVisitor visitor = new SQLVisitorImpl();
    SelectQueryRequest request = ((List<SelectQueryRequest>) visitor.visit(parser.parse())).get(0);

    var result = request.execute(db);
    assertEquals(2, result.getAttrs().size());
  }

  @Test
  public void test24() {
    Database db = Manager.getInstance().getDatabases().get("thss");

    String input = "SELECT hello.a, world.e, world.d FROM hello JOIN world ON hello.a = world.a WHERE world.d = 114514;";
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(input));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    SQLVisitor visitor = new SQLVisitorImpl();
    SelectQueryRequest request = ((List<SelectQueryRequest>) visitor.visit(parser.parse())).get(0);

    var result = request.execute(db);
    assertEquals(1, result.getAttrs().size());
  }

  @Test
  public void test80() {
    Database db = Manager.getInstance().getDatabases().get("thss");

    String input = "DELETE FROM hello WHERE c = 'nihao';";
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(input));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    SQLVisitor visitor = new SQLVisitorImpl();
    DeleteQueryRequest request = ((List<DeleteQueryRequest>) visitor.visit(parser.parse())).get(0);

    var result = request.execute(db);
    assertEquals("1", result.getAttrs().get(0).getCells().get(0).getValue());
  }

  @Test
  public void test90() {
    Database db = Manager.getInstance().getDatabases().get("thss");

    String input = "DROP TABLE hello;";
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(input));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    SQLVisitor visitor = new SQLVisitorImpl();
    DropQueryRequest request = ((List<DropQueryRequest>) visitor.visit(parser.parse())).get(0);

    var result = request.execute(db);
    assertEquals("1", result.getAttrs().get(0).getCells().get(0).getValue());
  }
}
