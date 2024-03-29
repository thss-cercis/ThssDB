package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.DeserializationException;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  @Getter
  private HashMap<String, Database> databases = new HashMap<>();
  private Database currentDatabase = null;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  @SneakyThrows
  private Manager() {
    // 读取已存在的数据库数据
    if (new File("metadata").exists()) {
      load();
    }
    // TODO: 默认创建数据库 thss，因为不想做数据库创建的语句
    createDatabaseIfNotExists("thss");
  }

  /**
   * * NOTE: not locked
   */
  private void finish() throws DeserializationException {
    try (DataOutputStream dos = new DataOutputStream(new FileOutputStream("metadata"))) {
      dos.writeInt(databases.size());
      for (Map.Entry<String, Database> entry : databases.entrySet()) {
        dos.writeUTF(entry.getKey());
        entry.getValue().shutdown();
      }
    } catch (IOException e) {
      throw new DeserializationException("Failed to save metadata.", e);
    }
  }

  private void load() throws DeserializationException {
    lock.writeLock().lock();
    try (DataInputStream fis = new DataInputStream(new FileInputStream("metadata"))) {
      int dbCount = fis.readInt();
      for (int i = 0; i < dbCount; ++i) {
        String databaseName = fis.readUTF();
        Database db = new Database(databaseName);

        databases.put(databaseName, db);
      }
      // TODO read logs
    } catch (IOException e) {
      throw new DeserializationException("Failed to load metadata.", e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void createDatabaseIfNotExists(String databaseName) {
    ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      databases.computeIfAbsent(databaseName, Database::new);
    } finally {
      writeLock.unlock();
    }
  }

  private void deleteDatabase(String databaseName) {
    ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      if (databases.containsKey(databaseName)) {
        Database db = databases.remove(databaseName);
        if (db == currentDatabase) {
          currentDatabase = null;
        }
      } else {
        throw new DatabaseNotExistException(String.format("Database %s does not exist", databaseName));
      }
    } finally {
      writeLock.unlock();
    }
  }

  public void switchDatabase(String databaseName) {
    ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    readLock.lock();
    try {
      Database db = databases.get(databaseName);
      if (db != null) {
        currentDatabase = db;
      } else {
        throw new DatabaseNotExistException(String.format("Database %s does not exist", databaseName));
      }
    } finally {
      readLock.unlock();
    }
  }

  public void shutdown() {
    // TODO interrupt all transactions
    lock.writeLock().lock();
    try {
      finish();
    } catch (Throwable ignored) {
    } finally {
      lock.writeLock().unlock();
    }
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();

    private ManagerHolder() {

    }
  }
}
