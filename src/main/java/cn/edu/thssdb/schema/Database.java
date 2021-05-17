package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DeserializationException;
import cn.edu.thssdb.exception.SerializationException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.utils.Pair;
import lombok.SneakyThrows;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

  private String name;
  private HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    recover();
  }

  private String getPath() {
    return "db_" + name;
  }

  private String getMetaPath() {
    return Paths.get(getPath(), "meta").toString();
  }

  private String getTablePath(String tableName) {
    return Paths.get(getPath(), "table_" + tableName).toString();
  }

  private void ensureDataDirectoryExists() throws FileNotFoundException {
    File file = new File(getPath());
    file.mkdirs();
    if (!file.exists()) {
      throw new FileNotFoundException("Unable to create data directory for database " + name);
    }
  }

  /**
   * Saves this database's meta data and tables.
   */
  @SneakyThrows
  private void persist() {
    lock.readLock().lock();
    try {
      ensureDataDirectoryExists();
      // save meta data
      File metaFile = new File(getMetaPath());
      try (DataOutputStream metaOutputStream = new DataOutputStream(new FileOutputStream(metaFile))) {
        // save database meta file
        // { tableCount, { tableName, columnCount, Column[columnCount] }[tableCont] }
        metaOutputStream.writeInt(tables.size());
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
          String tableName = entry.getKey();
          Table table = entry.getValue();
          metaOutputStream.writeUTF(tableName);
          metaOutputStream.writeInt(table.columns.length);
          for (int c = 0; c < table.columns.length; ++c) {
            table.columns[c].save(metaOutputStream);
          }
          // save table
          File tableFile = new File(getTablePath(tableName));
          if (tableFile.exists()) {
            try (FileInputStream fileInputStream = new FileInputStream(tableFile)) {
              table.recover(fileInputStream);
            }
          }
        }
      }
    } catch (FileNotFoundException e) {
      throw new SerializationException("Failed to persist database " + name, e);
    } finally {
      lock.readLock().unlock();
    }
    // TODO
  }

  public void createTable(String name, Column[] columns) {
    // TODO
  }

  public void drop() {
    // TODO
  }

  public String select(QueryTable[] queryTables) {
    // TODO
    QueryResult queryResult = new QueryResult(queryTables);
    return null;
  }

  @SneakyThrows
  private void recover() {
    try {
      ensureDataDirectoryExists();
      // load meta data
      File metaFile = new File(getMetaPath());
      if (!metaFile.exists()) {
        return;
      }
      ArrayList<Pair<String, Column[]>> tables = new ArrayList<>();
      try (DataInputStream metaInputStream = new DataInputStream(new FileInputStream(metaFile))) {
        // read database meta file
        // { tableCount, { tableName, columnCount, Column[columnCount] }[tableCont] }
        int tableCount = metaInputStream.readInt();
        for (int i = 0; i < tableCount; ++i) {
          String tableName = metaInputStream.readUTF();
          int columnCount = metaInputStream.readInt();
          Column[] columns = new Column[columnCount];
          for (int c = 0; c < columnCount; ++c) {
            columns[c] = Column.recover(metaInputStream);
          }
          tables.add(new Pair<>(tableName, columns));
        }
      }
      // load tables according to meta data
      for (Pair<String, Column[]> tableMeta : tables) {
        Table table = new Table(name, tableMeta.left, tableMeta.right);
        File tableFile = new File(getTablePath(tableMeta.left));
        if (tableFile.exists()) {
          try (FileInputStream fileInputStream = new FileInputStream(tableFile)) {
            table.recover(fileInputStream);
          }
        }
      }
    } catch (IOException e) {
      throw new DeserializationException("Failed to recover database " + name, e);
    }
  }

  public void quit() {
    persist();
  }
}
