package de.tuda.dmdb.storage.types;

import de.tuda.dmdb.storage.types.exercise.SQLInteger;

/**
 * Internal SQL value to store pointers to records in a page
 *
 * @author cbinnig
 */
public class SQLRowIdentifier extends AbstractSQLValue {

  private static final long serialVersionUID = 1L;

  public static int LENGTH = 2 * SQLIntegerBase.LENGTH; // fixed length

  private SQLInteger page; // page number
  private SQLInteger slot; // slot number

  /** Constructor with default value */
  public SQLRowIdentifier() {
    super(EnumSQLType.SqlRowIdentifier, LENGTH);

    this.page = new SQLInteger();
    this.slot = new SQLInteger();
  }

  /**
   * Constructor with page and slot number
   *
   * @param page page number
   * @param slot slot number
   */
  public SQLRowIdentifier(SQLInteger page, SQLInteger slot) {
    super(EnumSQLType.SqlRowIdentifier, LENGTH);

    this.page = page;
    this.slot = slot;
  }

  /**
   * Get page number of SQLowIdentifier
   *
   * @return the page number
   */
  public SQLInteger getPage() {
    return page;
  }

  /**
   * Set page number of SQLowIdentifier
   *
   * @param page page number
   */
  public void setPage(SQLInteger page) {
    this.page = page;
  }

  /**
   * Get slot number of SQLowIdentifier
   *
   * @return the slot number
   */
  public SQLInteger getSlot() {
    return slot;
  }

  /**
   * Set slot number of SQLowIdentifier
   *
   * @param slot slot number
   */
  public void setSlot(SQLInteger slot) {
    this.slot = slot;
  }

  @Override
  public int compareTo(AbstractSQLValue o) {
    SQLRowIdentifier cmp = (SQLRowIdentifier) o;

    if (cmp.page.compareTo(this.page) > 0) return 1;
    else if (cmp.page.compareTo(this.page) < 0) return -1;

    if (cmp.slot.compareTo(this.slot) > 0) return 1;
    else if (cmp.slot.compareTo(this.slot) < 0) return -1;

    return 0;
  }

  @Override
  public boolean equals(Object o) {
    SQLRowIdentifier cmp = (SQLRowIdentifier) o;
    if (this.page == cmp.page && this.slot == cmp.slot) return true;

    return false;
  }

  @Override
  public byte[] serialize() {
    byte[] data = new byte[LENGTH];
    System.arraycopy(this.page.serialize(), 0, data, 0, SQLInteger.LENGTH);
    System.arraycopy(this.slot.serialize(), 0, data, SQLInteger.LENGTH, SQLInteger.LENGTH);
    return data;
  }

  @Override
  public void deserialize(byte[] data) {
    byte[] tmp = new byte[SQLInteger.LENGTH];
    System.arraycopy(data, 0, tmp, 0, SQLInteger.LENGTH);
    this.page.deserialize(tmp);
    System.arraycopy(data, SQLInteger.LENGTH, tmp, 0, SQLInteger.LENGTH);
    this.slot.deserialize(tmp);
  }

  @Override
  public String toString() {
    return this.page.toString() + "," + this.slot.toString();
  }

  @Override
  public void parseValue(String data) {
    String[] tmp = data.split(",");
    this.page.parseValue(tmp[0]);
    this.page.parseValue(tmp[1]);
  }

  @Override
  public AbstractSQLValue clone() {
    return new SQLRowIdentifier(this.page.clone(), this.slot.clone());
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
