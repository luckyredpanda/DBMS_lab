package de.tuda.dmdb.operator;

import de.tuda.dmdb.access.AbstractIndex;
import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.storage.types.AbstractSQLValue;

/**
 * Scan Operator using an index to lookup a range.
 *
 * @param <T> The AbstractSQLValue that is used for the key column in the index
 */
public abstract class RangeIndexScanBase<T extends AbstractSQLValue> extends TableScanBase {
  protected AbstractIndex<T> index; // The index to use for the range scan
  protected T lower;
  protected T upper;

  protected RangeIndexScanBase(AbstractTable table, AbstractIndex<T> index, T lower, T upper) {
    super(table);

    this.index = index;
    this.lower = lower;
    this.upper = upper;
  }

  /**
   * Return the index that is scanned by this operator Used for debugging and testing purposes
   *
   * @return the index
   */
  public AbstractIndex<T> getIndex() {
    return index;
  }

  /**
   * Return the lower constant that is used for looking up by this operator Used for debugging and
   * testing purposes
   *
   * @return the lower constant value to compare to
   */
  public T getLowerLimit() {
    return lower;
  }
  /**
   * Return the upper constant that is used for looking up by this operator Used for debugging and
   * testing purposes
   *
   * @return the upper constant value to compare to
   */
  public T getUpperLimit() {
    return upper;
  }
}
