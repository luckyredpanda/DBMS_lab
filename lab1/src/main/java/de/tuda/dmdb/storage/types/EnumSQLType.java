package de.tuda.dmdb.storage.types;

import de.tuda.dmdb.storage.types.exercise.SQLBigInteger;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;

/**
 * Enumeration of implemented types for SqlValues
 *
 * @author cbinnig
 */
public enum EnumSQLType {
  SqlInteger(new SQLInteger()),
  SqlBigInteger(new SQLBigInteger()),
  SqlVarchar(new SQLVarchar(255)),
  SqlRowIdentifier(new SQLRowIdentifier());

  private AbstractSQLValue prototype = null;

  EnumSQLType(AbstractSQLValue prototype) {
    this.prototype = prototype;
  }

  public AbstractSQLValue getInstance(int length) {
    AbstractSQLValue value = this.prototype.clone();
    if (length > 0) value.setMaxLength(length);

    return value;
  }
}
