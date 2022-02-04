package de.tuda.dmdb.storage.types;

import java.io.IOException;
import java.io.Serializable;

/**
 * Defines the interface for a value stored in a record
 *
 * @author cbinnig
 */
public abstract class AbstractSQLValue
    implements Comparable<AbstractSQLValue>, Cloneable, Serializable {

  private static final long serialVersionUID = 1L;
  protected EnumSQLType type;
  protected int maxLength;
  protected boolean isFixedLength = true;

  /**
   * Creates an SQLValue with a given type and fixed length in bytes
   *
   * @param type SqlType for the value
   * @param maxLength Fixed length in bytes
   */
  public AbstractSQLValue(EnumSQLType type, int maxLength) {
    this.type = type;
    this.maxLength = maxLength;
  }

  /**
   * Creates an SQLValue with a given type and max. length in bytes. Also indicates if sql value is
   * a fixed length or variable length value
   *
   * @param type SqlType for the value
   * @param maxLength Max. length in bytes
   * @param isFixedLength Fixed or variable length value
   */
  public AbstractSQLValue(EnumSQLType type, int maxLength, boolean isFixedLength) {
    this.type = type;
    this.maxLength = maxLength;
    this.isFixedLength = isFixedLength;
  }

  /**
   * Returns the SQLType of this SQLValue
   *
   * @return an instance of EnumSQLType corresponding to the type of the SQL Value
   */
  public EnumSQLType getType() {
    return type;
  }

  /**
   * Sets SQLType of this SQLValue
   *
   * @param type The EnumSQLType to set as the type
   */
  public void setType(EnumSQLType type) {
    this.type = type;
  }

  /**
   * Returns max. length in bytes
   *
   * @return the max length in bytes of this SQLValue
   */
  public int getMaxLength() {
    return maxLength;
  }

  /**
   * Sets max. length in bytes
   *
   * @param length of value in bytes
   */
  public void setMaxLength(int length) {
    this.maxLength = length;
  }

  /**
   * Returns fixed length of value in bytes
   *
   * @return the fixed length of the SQLValue in bytes
   */
  public abstract int getFixedLength();

  /**
   * Returns variable length of value in bytes
   *
   * @return the variable length of the SQLValue
   */
  public abstract int getVariableLength();

  /**
   * Returns if a value has fixed length
   *
   * @return True if value has fixed length, else False
   */
  public boolean isFixedLength() {
    return isFixedLength;
  }

  /**
   * Serialize the value into a byte array
   *
   * @return a byte array containing the serialized value
   */
  public abstract byte[] serialize();

  /**
   * Deserialize the value from a byte array
   *
   * @param data the byte-array to deserialize
   */
  public abstract void deserialize(byte[] data);

  /**
   * Use special handling during the serialization as defined by the java.io.Serializable interface
   *
   * @param out The ObjectOutputStream to write the serialized data to
   * @throws IOException can be thrown by the ObjectOutputStream.write() method
   */
  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    // default serialization
    out.defaultWriteObject();
    out.write(this.serialize());
  }

  /**
   * Use special handling during the deserialization as defined by the java.io.Serializable
   * interface
   *
   * @param in The ObjectInputStream to read data from
   * @throws IOException can be thrown by the ObjectInputStream
   * @throws ClassNotFoundException can be thrown when attempting the deserialization from the read
   *     data
   */
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    // default deserialization
    in.defaultReadObject();
    byte[] serializedData = new byte[this.getMaxLength()];
    in.read(serializedData);
    this.deserialize(serializedData);
  }

  /**
   * Constructs the value from a string representation
   *
   * @param data The data to parse as an SQL value
   */
  public abstract void parseValue(String data);

  @Override
  public abstract AbstractSQLValue clone();
}
