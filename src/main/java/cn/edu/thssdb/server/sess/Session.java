/*
 * Session 的定义.
 *
 * Author: AyajiLin
 * Date: 2021-05-02
 */
package cn.edu.thssdb.server.sess;

import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
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
  private List<String> statementList;

  // TODO: 写死数据库
  private static final Database currentDatabase;

  static {
    currentDatabase = Manager.getInstance().getDatabases().get("thss");
  }

  public void beginTransaction() {
    transactionId = UUID.randomUUID().toString();
  }

  public void commitTransaction() {
    currentDatabase.getDbLock().writeLock().lock();
    statementList.forEach(currentDatabase::execute);
    currentDatabase.getDbLock().writeLock().unlock();
    currentDatabase.writeLog(transactionId, "commit");
    currentDatabase.getCommitted().add(transactionId);
    transactionId = null;
    statementList.clear();
  }

  public void addLog(String statement) {
    currentDatabase.writeLog(transactionId, statement);
    statementList.add(statement);
  }

  @Override
  public void close() {
  }
}
