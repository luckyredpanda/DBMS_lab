package de.tuda.dmdb.storage.types;

/** SQLValue representing NULL in the database */
public class SQLNull extends AbstractSQLValue {

  private static final long serialVersionUID = 1L;

  public static int LENGTH = 0; // fixed length

  /** Constructor with default value */
  public SQLNull() {
    super(EnumSQLType.SqlNull, LENGTH);
  }

  @Override
  public int compareTo(AbstractSQLValue o) {
    if (o instanceof SQLNull) return 0;

    return 1;
  }

  @Override
  public boolean equals(Object o) {
    return (o instanceof SQLNull);
  }

  @Override
  public byte[] serialize() {
    return new byte[LENGTH];
  }

  @Override
  public void deserialize(byte[] data) {
    // do nothing
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    return (int) serialVersionUID * prime;
  }

  @Override
  public String toString() {
    return "NULL";
  }

  @Override
  public void parseValue(String data) {
    // do nothing
  }

  @Override
  public AbstractSQLValue clone() {
    return new SQLNull();
  }

  @Override
  public int getFixedLength() {
    return LENGTH;
  }

  @Override
  public int getVariableLength() {
    return 0;
  }
}
