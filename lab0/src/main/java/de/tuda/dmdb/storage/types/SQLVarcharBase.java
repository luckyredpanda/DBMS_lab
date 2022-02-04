package de.tuda.dmdb.storage.types;

/**
 * SQL varchar value
 *
 * @author cbinnig
 */
public abstract class SQLVarcharBase extends AbstractSQLValue {

  private static final long serialVersionUID = 1L;

  public static int LENGTH =
      2 * SQLIntegerBase.LENGTH; // fixed length. 1 Int for size & 1 Int for pointer

  protected byte[] data; // cache for byte representation
  protected String value; // String value

  /**
   * Constructor with default value and max. length
   *
   * @param maxLength Maximum number of bytes of a varchar
   */
  public SQLVarcharBase(int maxLength) {
    super(EnumSQLType.SqlVarchar, maxLength, false);
    this.value = "";
    this.data = this.serialize();
  }

  /**
   * Constructor with string value and max. length
   *
   * @param value The value to set for this varchar
   * @param maxLength The maximum number of bytes the varchar can store
   */
  public SQLVarcharBase(String value, int maxLength) {
    super(EnumSQLType.SqlVarchar, maxLength, false);
    this.value = value;
    this.data = this.serialize();
  }

  /**
   * Get string value of SQLVarchar
   *
   * @return Return the value of this varchar as String
   */
  public String getValue() {
    return value;
  }

  /**
   * Set string value of SQLVarchar
   *
   * @param value The string value to set for this varchar
   */
  public void setValue(String value) {
    this.value = value;
    this.data = this.serialize();
  }

  @Override
  public int getFixedLength() {
    return LENGTH;
  }

  @Override
  public int getVariableLength() {
    return this.data.length;
  }

  @Override
  public String toString() {
    return this.value;
  }

  @Override
  public boolean equals(Object obj) {
    // it does the check for null  - "null instanceof [type]" always returns false
    if (!(obj instanceof SQLVarcharBase)) return false;
    if (obj == this) return true;

    SQLVarcharBase cmp = (SQLVarcharBase) obj;
    // return this.getMaxLength() == cmp.getMaxLength() && this.value.equals(cmp.value);
    return this.value.equals(
        cmp.value); // ignoring the length turned out to be more practical in most cases
  }

  @Override
  public int compareTo(AbstractSQLValue o) {
    SQLVarcharBase cmp = (SQLVarcharBase) o;
    return this.value.compareTo(cmp.value);
  }

  @Override
  public int hashCode() {
    return this.value.hashCode();
  }

  @Override
  public void parseValue(String data) {
    this.value = data;
  }
}
