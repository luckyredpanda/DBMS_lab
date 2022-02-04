package de.tuda.dmdb.operator;

import de.tuda.dmdb.storage.types.AbstractSQLValue;

public abstract class SelectionBase extends UnaryOperator {
  protected int attribute;
  protected AbstractSQLValue constant;

  protected SelectionBase(Operator child, int attribute, AbstractSQLValue constant) {
    super(child);

    this.attribute = attribute;
    this.constant = constant;
  }
}
