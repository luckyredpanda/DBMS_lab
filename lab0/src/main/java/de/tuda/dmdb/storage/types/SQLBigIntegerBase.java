package de.tuda.dmdb.storage.types;

/**
 * SQL integer value
 *
 * @author cbinnig
 */
public abstract class SQLBigIntegerBase extends AbstractSQLValue {

  private static final long serialVersionUID = 1L;
  public static int LENGTH = 8; // fixed length
  protected long value = 0; // integer value

  /** Constructor with default value */
  public SQLBigIntegerBase() {
    super(EnumSQLType.SqlBigInteger, LENGTH);
    this.value = 0;
  }

  /**
   * Construct a BigInteger with the passed value
   *
   * @param value BigInteger value
   */
  public SQLBigIntegerBase(long value) {
    super(EnumSQLType.SqlBigInteger, LENGTH);
    this.value = value;
  }

  /**
   * Returns long value of SQLBigInteger
   *
   * @return The value as long variable
   */
  public long getValue() {
    return value;
  }

  /**
   * Sets long value of SQLBigInteger
   *
   * @param value The value of type long to set for the BigInteger
   */
  public void setValue(long value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "" + this.value;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (!(obj instanceof SQLBigIntegerBase)) return false;
    if (obj == this) return true;

    SQLBigIntegerBase cmp = (SQLBigIntegerBase) obj;
    return this.value == cmp.value;
  }

  @Override
  public int compareTo(AbstractSQLValue o) {
    SQLBigIntegerBase cmp = (SQLBigIntegerBase) o;
    return (int) (this.value - cmp.value);
  }

  @Override
  public int hashCode() {
    return (int) this.value;
  }

  @Override
  public void parseValue(String data) {
    this.value = Long.parseLong(data);
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
