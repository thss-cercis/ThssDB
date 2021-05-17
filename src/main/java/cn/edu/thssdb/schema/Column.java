package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DeserializationException;
import cn.edu.thssdb.type.ColumnType;
import lombok.Data;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

@Data
public class Column implements Comparable<Column> {
  private final String name;
  private final ColumnType type;
  private final int primary;
  private final boolean notNull;
  private final int maxLength;

  public static Column recover(DataInputStream dataInputStream) throws IOException {
    String name = dataInputStream.readUTF();
    int columnType = dataInputStream.readInt();
    if (columnType > ColumnType.values().length) {
      throw new DeserializationException("Unexpected column type", new ArrayIndexOutOfBoundsException(columnType));
    }
    ColumnType type = ColumnType.values()[columnType];
    int primary = dataInputStream.readInt();
    boolean notNull = dataInputStream.readBoolean();
    int maxLength = dataInputStream.readInt();
    return new Column(name, type, primary, notNull, maxLength);
  }

  public Column(String name, ColumnType type, int primary, boolean notNull, int maxLength) {
    this.name = name;
    this.type = type;
    this.primary = primary;
    this.notNull = notNull;
    this.maxLength = maxLength;
  }

  public void save(DataOutputStream dataOutputStream) throws IOException {
    dataOutputStream.writeUTF(name);
    int typeInt = -1;
    for (int i = 0; i < ColumnType.values().length; ++i) {
      if (ColumnType.values()[i] == type) {
        typeInt = i;
        break;
      }
    }
    dataOutputStream.writeInt(typeInt);
    dataOutputStream.writeInt(primary);
    dataOutputStream.writeBoolean(notNull);
    dataOutputStream.writeInt(maxLength);
  }

  @Override
  public int compareTo(Column e) {
    return name.compareTo(e.name);
  }

  public String toString() {
    return name + ',' + type + ',' + primary + ',' + notNull + ',' + maxLength;
  }
}
