package de.tuda.dmdb.storage.types.exercise;

import de.tuda.dmdb.storage.types.SQLBigIntegerBase;

import java.nio.ByteBuffer;

/**
 * SQL long value
 *
 * @author lthostrup
 */
public class SQLBigInteger extends SQLBigIntegerBase {
  private static final long serialVersionUID = 1L;

  /** Constructor with default value */
  public SQLBigInteger() {
    super();
  }

  /**
   * Constructor with value
   *
   * @param value Integer value
   */
  public SQLBigInteger(long value) {
    super(value);
  }

  @Override
  public byte[] serialize() {
    // TODO: implement this method
    byte[] b = new byte[8];
    b[7] = (byte) (super.value & 0xff);
    b[6] = (byte) ((super.value >>> 8) & 0xff);
    b[5] = (byte) ((super.value >>> 16) & 0xff);
    b[4] = (byte) ((super.value >>> 24) & 0xff);
    b[3] = (byte) ((super.value >>> 32) & 0xff);
    b[2] = (byte) ((super.value >>> 40) & 0xff);
    b[1] = (byte) ((super.value >>> 48) & 0xff);
    b[0] = (byte) ((super.value >>> 56) & 0xff);
    return b;
  }

  @Override
  public void deserialize(byte[] bytes) {
    // TODO: implement this method
    value=0;
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.put(bytes, 0, bytes.length);
    buffer.flip();
    value= buffer.getLong();
  }

  @Override
  public SQLBigInteger clone() {
    return new SQLBigInteger(this.value);
  }
}
