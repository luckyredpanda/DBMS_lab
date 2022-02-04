package de.tuda.dmdb.operator;

import de.tuda.dmdb.access.AbstractIndex;
import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.storage.types.AbstractSQLValue;

/**
 * Scan Operator expecting only one item to be returned, identified by T constant. Uses the index to
 * lookup the value.
 *
 * @param <T> The AbstractSQLValue that is used for the key column in the index
 */
public abstract class IndexScanBase<T extends AbstractSQLValue> extends TableScanBase {
  protected AbstractIndex<T> index; // The index to use to for the scan operation
  protected T constant;

  protected IndexScanBase(AbstractTable table, AbstractIndex<T> index, T constant) {
    super(table);

    this.index = index;
    this.constant = constant;
  }

  /**
   * Return the index that is scanned by this operator. Used for debugging and testing purposes
   *
   * @return the index
   */
  public AbstractIndex<T> getIndex() {
    return index;
  }

  /**
   * Return the constant that is used for looking up of records by this operator. Used for debugging
   * and testing purposes
   *
   * @return the constant value to compare to
   */
  public T getConstant() {
    return constant;
  }
}
