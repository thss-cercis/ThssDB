/*
 * Session 的定义.
 *
 * Author: AyajiLin
 * Date: 2021-05-02
 */
package cn.edu.thssdb.server.sess;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLVisitor;
import cn.edu.thssdb.parser.SQLVisitorImpl;
import cn.edu.thssdb.query.IQueryRequest;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@AllArgsConstructor
@Data
public class Session implements Closeable {
  private long sessionID;
  private String username;
  @Nullable
  @Getter
  private String transactionId;
  @Getter
  private List<String> logs;

  // TODO: 写死数据库
  private static final Database currentDatabase;

  static {
    currentDatabase = Manager.getInstance().getDatabases().get("thss");
  }

  public void beginTransaction() {
    transactionId = UUID.randomUUID().toString();
    writeLog("begin");
  }

  public void commitTransaction() {
    ReentrantReadWriteLock lock = currentDatabase.getLock();
    lock.writeLock().lock();
    logs.forEach(statement -> {
      SQLLexer lexer = new SQLLexer(CharStreams.fromString(statement));
      CommonTokenStream tokens = new CommonTokenStream(lexer);
      SQLParser parser = new SQLParser(tokens);
      SQLVisitor<?> visitor = new SQLVisitorImpl();
      List<?> reqList = (List<?>) visitor.visit(parser.parse());
      IQueryRequest req = (IQueryRequest) reqList.get(0);
      req.execute(currentDatabase);
    });
    try {
      writeLogWithoutLock("commit");
      currentDatabase.persistWithoutLock();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      transactionId = null;
      logs.clear();
      lock.writeLock().unlock();
    }
  }

  private void writeLogWithoutLock(String statement) throws IOException {
    FileOutputStream fos = new FileOutputStream(currentDatabase.getLogFile(), true);
    fos.write((transactionId + "\n").getBytes(StandardCharsets.UTF_8));
    fos.write((statement + "\n").getBytes(StandardCharsets.UTF_8));
  }

  private void writeLog(String statement) {
    ReentrantReadWriteLock lock = currentDatabase.getLock();
    lock.writeLock().lock();
    try {
      writeLogWithoutLock(statement);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void addLog(String statement) {
    ReentrantReadWriteLock lock = currentDatabase.getLock();
    lock.writeLock().lock();
    try {
      writeLogWithoutLock(statement);
      logs.add(statement);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void close() {
  }
}
