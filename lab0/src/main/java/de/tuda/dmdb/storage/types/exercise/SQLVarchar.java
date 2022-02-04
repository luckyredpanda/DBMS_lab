package de.tuda.dmdb.storage.types.exercise;

import de.tuda.dmdb.storage.types.SQLVarcharBase;

/**
 * SQL varchar value
 *
 * @author cbinnig
 */
public class SQLVarchar extends SQLVarcharBase {

  private static final long serialVersionUID = 1L;

  /**
   * Constructor with default value and max. length
   *
   * @param maxLength Maximum number of bytes of a varchar
   */
  public SQLVarchar(int maxLength) {
    super(maxLength);
  }

  /**
   * Constructor with string value and max. length
   *
   * @param value The string value to set for this varchar
   * @param maxLength The maximum number of bytes the varchar can store
   */
  public SQLVarchar(String value, int maxLength) {
    super(value, maxLength);
  }

  @Override
  public byte[] serialize() {
    // TODO: implement this method
    byte []b=value.getBytes();
    return b;
  }

  @Override
  public void deserialize(byte[] data) {
    // TODO: implement this method
    value=new String(data);
  }

  @Override
  public SQLVarchar clone() {
    return new SQLVarchar(this.value, this.maxLength);
  }
}
