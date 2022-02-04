package de.tuda.dmdb.storage.types;

/**
 * SQL integer value
 *
 * @author cbinnig
 */
public abstract class SQLIntegerBase extends AbstractSQLValue {

  private static final long serialVersionUID = 1L;
  public static int LENGTH = 4; // fixed length
  protected int value = 0; // integer value

  /** Constructor with default value */
  public SQLIntegerBase() {
    super(EnumSQLType.SqlInteger, LENGTH);
    this.value = 0;
  }

  /**
   * Constructor with value
   *
   * @param value Integer value
   */
  public SQLIntegerBase(int value) {
    super(EnumSQLType.SqlInteger, LENGTH);
    this.value = value;
  }

  /**
   * Returns integer value of SQLInteger
   *
   * @return the value of this SQLValue as integer
   */
  public int getValue() {
    return value;
  }

  /**
   * Sets integer value of SQLInteger
   *
   * @param value The integer value to set
   */
  public void setValue(int value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "" + this.value;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (!(obj instanceof SQLIntegerBase)) return false;
    if (obj == this) return true;

    SQLIntegerBase cmp = (SQLIntegerBase) obj;
    return this.value == cmp.value;
  }

  @Override
  public int compareTo(AbstractSQLValue o) {
    SQLIntegerBase cmp = (SQLIntegerBase) o;
    return this.value - cmp.value;
  }

  @Override
  public int hashCode() {
    return this.value;
  }

  @Override
  public void parseValue(String data) {
    this.value = Integer.parseInt(data);
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
