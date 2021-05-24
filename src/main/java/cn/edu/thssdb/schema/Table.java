package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DeserializationException;
import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.InvalidColumnSchemaException;
import cn.edu.thssdb.exception.SerializationException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.utils.Pair;
import lombok.Getter;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;

public class Table implements Iterable<Row> {
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  public final BPlusTree<Entry, Row> index = new BPlusTree<>();
  @Getter
  private final String databaseName;
  @Getter
  public final String tableName;
  @Getter
  public final Column[] columns;
  @Getter
  private final int primaryIndex;

  /**
   * Constructs a table with metadata. Metadata cannot be modified.
   *
   * @param databaseName name of the database
   * @param tableName    name of the table
   * @param columns      metadata of the columns
   */
  public Table(String databaseName, String tableName, Column[] columns) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = columns.clone();
    int primaryIndexTmp = -1;
    for (int i = 0; i < columns.length; ++i) {
      if (columns[i].getPrimary() != 0) {
        if (primaryIndexTmp != -1) {
          throw new InvalidColumnSchemaException(String.format("Double primary key for table %s", tableName));
        }
        primaryIndexTmp = i;
      }
    }
    if (primaryIndexTmp == -1) {
      throw new InvalidColumnSchemaException(String.format("No primary key exists for table %s", tableName));
    }
    this.primaryIndex = primaryIndexTmp;
  }

  /**
   * Deserializes rows from inputStream. When the table is fully read, position of the inputStream will
   * stay at one byte after table end.
   * <p>
   * No read or write lock is used on this method, thus the caller should take care of the lock.
   *
   * @param inputStream the stream to read from
   * @throws DeserializationException if deserialization fails
   */
  public void recover(InputStream inputStream) throws DeserializationException {
    try {
      ObjectInputStream ois = new ObjectInputStream(inputStream);
      Object object = ois.readObject();
      @SuppressWarnings("unchecked")
      ArrayList<Row> rows = (ArrayList<Row>) object;
      rows.forEach(row -> index.put(row.entries.get(primaryIndex), row));
    } catch (IOException | ClassNotFoundException | ClassCastException | ArrayIndexOutOfBoundsException exception) {
      exception.printStackTrace();
      throw new DeserializationException(String.format("Failed to read table %s", tableName), exception);
    }
  }

  /**
   * Saves rows to outputStream.
   * <p>
   * No read or write lock is used on this method, thus the caller should take care of the lock.
   *
   * @param outputStream output stream to write to
   * @throws SerializationException if serialization fails
   */
  public void save(OutputStream outputStream) throws SerializationException {
    try {
      ObjectOutputStream oos = new ObjectOutputStream(outputStream);
      Iterator<Pair<Entry, Row>> iter = index.iterator();
      ArrayList<Row> rows = new ArrayList<>();
      iter.forEachRemaining(pair -> rows.add(pair.right));
      oos.writeObject(rows);
    } catch (IOException exception) {
      throw new SerializationException(String.format("Failed to save table %s", tableName), exception);
    }
  }

  /**
   * Acquire a lock for writing to this table. {@link ReentrantReadWriteLock.WriteLock#lock()} need to be called on the
   * return lock object to lock the table.
   *
   * @return the write lock acquired.
   */
  public ReentrantReadWriteLock.WriteLock acquireWriteLock() {
    return lock.writeLock();
  }

  /**
   * Acquire a lock for reading this table. {@link ReentrantReadWriteLock.ReadLock#lock()} need to be called on the
   * return lock object to lock the table.
   *
   * @return the read lock acquired.
   */
  public ReentrantReadWriteLock.ReadLock acquireReadLock() {
    return lock.readLock();
  }

  /**
   * Insert a row into the table.
   * <p>
   * * NOTE: Schema check is not done here. Since table schema would not be edited, this check can be done prior to
   * insertion on all rows to be inserted.
   *
   * @param rawData raw data to be inserted
   */
  public void insert(Entry[] rawData) {
    index.put(rawData[primaryIndex], new Row(rawData));
  }

  public void delete(Entry primaryKey) {
    index.remove(primaryKey);
  }

  public void update(Entry primaryKey, Iterator<Pair<Column, Entry>> columns) {
    Row rowData = index.get(primaryKey);
    if (rowData == null) {
      return;
    }
    while (columns.hasNext()) {
      Pair<Column, Entry> col = columns.next();
      Column colSchema = col.left;
      Entry colEntry = col.right;
      int colIdx = IntStream.range(0, this.columns.length)
        .filter(i -> this.columns[i] == colSchema)
        .findFirst()
        .orElse(-1);
      if (colIdx < 0) {
        throw new InvalidColumnSchemaException("column not found: " + colSchema.toString());
      }
      if (colSchema.isNotNull() && colEntry.value == null) {
        throw new InvalidColumnSchemaException("null value could not be assigned to column: " + colSchema.toString());
      }
      rowData.entries.set(colIdx, colEntry);
    }
  }

  private class TableIterator implements Iterator<Row> {
    private Iterator<Pair<Entry, Row>> iterator;

    TableIterator(Table table) {
      this.iterator = table.index.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      return iterator.next().right;
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }
}
