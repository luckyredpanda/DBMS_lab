package de.tuda.dmdb.storage;

import de.tuda.dmdb.storage.types.AbstractSQLValue;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Vector;

/**
 * Defines the interface for a record stored in a page
 *
 * @author cbinnig
 */
public abstract class AbstractRecord implements Cloneable, Serializable {

  private static final long serialVersionUID = 1L;
  // store attributes of a record (ie. columns)
  // AbstractSQLValue provides access to the type and the actual value of an attribute in a record
  protected AbstractSQLValue[] values;

  /**
   * Creates a record with a given number of attributes
   *
   * @param length number of attributes
   */
  public AbstractRecord(int length) {
    this.values = new AbstractSQLValue[length];
  }

  /**
   * Set value i of record
   *
   * @param i The index of the attribute/column to set
   * @param value The SQLValue to set for the i-th column/attribute
   */
  public void setValue(int i, AbstractSQLValue value) {
    this.values[i] = value;
  }

  /**
   * Return value of attribute with a given number
   *
   * @param i attribute number
   * @return The value of the attribute with index i
   */
  public AbstractSQLValue getValue(int i) {
    return this.values[i];
  }

  /**
   * Returns all values of the record as array
   *
   * @return An array of AbstractSQLValue
   */
  public AbstractSQLValue[] getValues() {
    return this.values;
  }

  /**
   * Keeps only listed column number. Pass null to keep no values.
   *
   * @param colNumber The index/number of column/attribute to keep
   */
  public void keepValue(Integer colNumber) {
    if (colNumber == null) {
      this.values = new AbstractSQLValue[0];
      return;
    }
    AbstractSQLValue[] tmpValues = this.values;
    this.values = new AbstractSQLValue[1];
    this.values[0] = tmpValues[colNumber];
  }

  /**
   * Keeps only listed column numbers
   *
   * @param colNumbers A List of column-numbers/indexes to keep
   */
  public void keepValues(Vector<Integer> colNumbers) {
    AbstractSQLValue[] tmpValues = this.values;

    this.values = new AbstractSQLValue[colNumbers.size()];
    int i = 0;
    for (Integer colNum : colNumbers) {
      this.values[i++] = tmpValues[colNum];
    }
  }

  /**
   * Returns the fixed size of a record needed for for storing in a slot of a page
   *
   * @return The fixed size of the record in bytes
   */
  public abstract int getFixedLength();

  /**
   * Returns the variable size of a record needed for storing the record in the variable part of a
   * page
   *
   * @return The variable length of the record in bytes
   */
  public abstract int getVariableLength();

  /**
   * Creates a new record form current record and rec by appending rec to the end of the current
   * record
   *
   * @param rec The record to append
   * @return A new record consisting of rec appended to this (current) record
   */
  public abstract AbstractRecord append(AbstractRecord rec);

  @Override
  public abstract AbstractRecord clone();

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(values);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    // check for self-comparison
    if (this == o) return true;

    // use instanceof instead of getClass here for two reasons
    // 1. if need be, it can match any supertype, and not just one class;
    // 2. it renders an explict check for "that == null" redundant, since
    // it does the check for null already - "null instanceof [type]" always
    // returns false. (See Effective Java by Joshua Bloch.)
    if (!(o instanceof AbstractRecord)) return false;

    AbstractRecord cmp = (AbstractRecord) o;

    if (this.values.length != cmp.getValues().length) return false;

    int i = 0;
    for (AbstractSQLValue cmpValue : cmp.getValues()) {
      // check if types match
      if (cmpValue.getType() != this.values[i].getType()) return false;

      // check if values match
      if (!cmpValue.equals(this.values[i])) return false;

      i++;
    }
    return true;
  }
}
