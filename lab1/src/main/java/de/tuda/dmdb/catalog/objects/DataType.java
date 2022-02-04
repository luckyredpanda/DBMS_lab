package de.tuda.dmdb.catalog.objects;

import de.tuda.dmdb.storage.types.EnumSQLType;

public class DataType {
  private EnumSQLType type;
  private int length = 0;

  public EnumSQLType getType() {
    return type;
  }

  public void setType(EnumSQLType type) {
    this.type = type;
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }
}
