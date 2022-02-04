package de.tuda.dmdb.operator;

import de.tuda.dmdb.access.AbstractTable;

public abstract class TableScanBase extends Operator {
  protected AbstractTable table;

  public TableScanBase(AbstractTable table) {
    super();

    this.table = table;
  }

  /**
   * Return the table that is scanned by this operator Used for debugging and testing purposes
   *
   * @return the table
   */
  public AbstractTable getTable() {
    return table;
  }
}
