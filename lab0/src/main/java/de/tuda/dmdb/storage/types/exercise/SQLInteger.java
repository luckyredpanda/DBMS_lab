package de.tuda.dmdb.storage.types.exercise;

import de.tuda.dmdb.storage.types.SQLIntegerBase;

/**
 * SQL integer value
 *
 * @author cbinnig
 */
public class SQLInteger extends SQLIntegerBase {

  private static final long serialVersionUID = 1L;

  /** Constructor with default value */
  public SQLInteger() {
    super();
  }

  /**
   * Constructor with value
   *
   * @param value Integer value
   */
  public SQLInteger(int value) {
    super(value);
  }

  @Override
  public byte[] serialize() {
    // TODO: implement this method
    byte[] b = new byte[4];
    b[3] = (byte) (super.value & 0xff);
    b[2] = (byte) (super.value >> 8 & 0xff);
    b[1] = (byte) (super.value >> 16 & 0xff);
    b[0] = (byte) (super.value >> 24 & 0xff);
    return b;
  }

  @Override
  public void deserialize(byte[] data) {
    // TODO: implement this method
    value=0;
    for(int i=0;i<data.length;i++){
      value += (data[i] & 0xff) << ((3-i)*8);
      }
  }

  @Override
  public SQLInteger clone() {
    return new SQLInteger(this.value);
  }
}
