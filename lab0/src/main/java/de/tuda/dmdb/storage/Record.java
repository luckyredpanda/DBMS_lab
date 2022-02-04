package de.tuda.dmdb.storage;

import de.tuda.dmdb.storage.types.AbstractSQLValue;

public class Record extends AbstractRecord {

  private static final long serialVersionUID = 1L;

  /**
   * Constructor for a record with a given number of attributes
   *
   * @param length The number of attributes that the record consists of
   */
  public Record(int length) {
    super(length);
  }

  @Override
  public int getFixedLength() {
    int length = 0;
    for (AbstractSQLValue value : this.values) {
      length += value.getFixedLength();
    }
    return length;
  }

  @Override
  public int getVariableLength() {
    int length = 0;
    for (AbstractSQLValue value : this.values) {
      length += value.getVariableLength();
    }
    return length;
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("(");
    for (AbstractSQLValue value : this.values) {
      buffer.append(value);
      buffer.append(" ");
    }
    buffer.append(")");
    return buffer.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (!(o instanceof Record)) return false;

    Record cmp = (Record) o;

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

  @Override
  public Record clone() {
    Record record = new Record(this.values.length);
    int i = 0;
    for (AbstractSQLValue value : this.values) {
      if (value != null) {
        record.values[i] = value.clone();
      }
      i++;
    }
    return record;
  }

  @Override
  public AbstractRecord append(AbstractRecord rec) {
    Record newRec = new Record(this.values.length + rec.values.length);

    int i = 0;
    for (AbstractSQLValue val : this.values) newRec.values[i++] = val;

    for (AbstractSQLValue val : rec.values) newRec.values[i++] = val;

    return newRec;
  }
}
