package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLVisitor;
import cn.edu.thssdb.parser.SQLVisitorImpl;
import cn.edu.thssdb.query.IQueryRequest;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.utils.Pair;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.var;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {
  @Getter
  private final String name;
  @Getter
  private final Map<String, Table> tables;
  @Getter
  ReentrantReadWriteLock dbLock;
  @Getter
  ReentrantReadWriteLock logLock;
  @Getter
  File logFile;
  @Getter
  List<String> committed;

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.dbLock = new ReentrantReadWriteLock();
    this.logLock = new ReentrantReadWriteLock();
    this.logFile = new File(getLogPath());
    this.committed = new ArrayList<>();
    recover();
  }

  private String getPath() {
    return "db_" + name;
  }

  private String getLogPath() {
    return Paths.get(getPath(), "log").toString();
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
    dbLock.readLock().lock();
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
          try (FileOutputStream fileOutputStream = new FileOutputStream(tableFile)) {
            table.save(fileOutputStream);
          }
        }
      }
    } catch (FileNotFoundException e) {
      throw new SerializationException("Failed to persist database " + name, e);
    } finally {
      dbLock.readLock().unlock();
    }
  }

  public void createTable(String name, Column[] columns) {
    dbLock.writeLock().lock();
    try {
      Table table = tables.get(name);
      if (table != null) {
        throw new TableAlreadyExistException("table already exists: " + name);
      } else if (columns == null || columns.length == 0) {
        throw new InvalidColumnSchemaException("invalid column schema for table: " + name);
      }
      Table newTable = new Table(this.name, name, columns);
      this.tables.put(name, newTable);
    } finally {
      dbLock.writeLock().unlock();
    }
  }

  public void drop() {
    // TODO: 不需要实现
  }

  public void dropTable(String name) {
    dbLock.writeLock().lock();
    try {
      Table table = tables.get(name);
      if (table == null) {
        throw new TableNotExistException("table not exists: " + name);
      }
      var mutex = table.acquireWriteLock();
      mutex.lock();
      try {
        tables.remove(name);
      } finally {
        mutex.unlock();
      }
    } finally {
      dbLock.writeLock().unlock();
    }
  }

  @SneakyThrows
  private void loadTable() {
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
        this.tables.put(table.tableName, table);
      }
    } catch (IOException e) {
      throw new DeserializationException("Failed to recover database " + name, e);
    }
  }

  public QueryResult execute(String statement) {
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(statement));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    SQLVisitor<?> visitor = new SQLVisitorImpl();
    List<?> reqList = (List<?>) visitor.visit(parser.parse());
    IQueryRequest req = (IQueryRequest) reqList.get(0);
    return req.execute(this);
  }

  public static void writeLogTo(FileOutputStream fos, String transactionId, String statement) throws IOException {
    fos.write((transactionId + "\n").getBytes(StandardCharsets.UTF_8));
    fos.write((statement + "\n").getBytes(StandardCharsets.UTF_8));
  }

  public void writeLog(String transactionId, String statement) {
    logLock.writeLock().lock();
    try (FileOutputStream fos = new FileOutputStream(logFile, true)) {
      writeLogTo(fos, transactionId, statement);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      logLock.writeLock().unlock();
    }
  }

  private void recover() {
    loadTable();
    List<String> log = new ArrayList<>();
    try (FileInputStream fis = new FileInputStream(logFile)) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
      while (true) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        log.add(line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    assert log.size() % 2 == 0;
    Set<String> redo = new HashSet<>();
    Map<String, List<String>> statementMap = new HashMap<>();
    List<String> committedList = new ArrayList<>();
    for (int i = 0; i < log.size(); i += 2) {
      String transactionId = log.get(i);
      String statement = log.get(i + 1);
      if (statement.equals("written")) {
        redo.remove(transactionId);
      } else {
        if (statement.equals("commit")) {
          redo.add(transactionId);
          committedList.add(transactionId);
        }
        List<String> statementList = statementMap.getOrDefault(transactionId, new ArrayList<>());
        statementList.add(statement);
        statementMap.put(transactionId, statementList);
      }
    }
    try (FileOutputStream fos = new FileOutputStream(logFile)) {
      for (String transactionId : committedList) {
        if (redo.contains(transactionId)) {
          for (String statement : statementMap.get(transactionId)) {
            writeLogTo(fos, transactionId, statement);
          }
          committed.add(transactionId);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    for (String transactionId : committedList) {
      if (redo.contains(transactionId)) {
        for (String statement : statementMap.get(transactionId)) {
          if (!statement.equals("commit")) {
            execute(statement);
          }
        }
      }
    }
  }

  public void shutdown() {
    persist();
    logLock.writeLock().lock();
    try (FileOutputStream fos = new FileOutputStream(logFile, true)) {
      for (String transactionId : committed) {
        writeLogTo(fos, transactionId, "written");
      }
      committed.clear();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      logLock.writeLock().unlock();
    }
  }
}
