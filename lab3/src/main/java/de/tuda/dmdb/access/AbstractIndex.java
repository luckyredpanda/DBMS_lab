package de.tuda.dmdb.access;

import de.tuda.dmdb.operator.TableScanBase;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import java.util.Iterator;

/**
 * Abstract class for indices. Defines methods that all indices should support. This is only a
 * single dimensional index, i.e., we can only index one column. This class assumes static indices
 * that are build once and not changed afterwards. If an index must be updated, it must be deleted
 * and re-build from scratch.
 */
public abstract class AbstractIndex<T extends AbstractSQLValue> {

  // The table to be indexed
  protected AbstractTable table;

  /**
   * Creates an index for a given table
   *
   * @param table the table over which the index is built
   */
  protected AbstractIndex(AbstractTable table) {
    this.table = table;
  }

  /**
   * Build an index by processing all records of the provided table
   *
   * @param table to be indexed
   */
  public abstract void bulkLoad(TableScanBase table);

  /**
   * Returns a record for a given key
   *
   * @param key the key to search an entry for
   * @return Iterator of records matching the key. Return an empty iterator if no matching records
   *     could be found. Unique indexes return iterators with only one item.
   */
  public abstract Iterator<RecordIdentifier> lookup(T key);

  /**
   * Returns an Iterator of RID of Records in the index tabled that fall into the specified range.
   * StartKey and endKey are AbstractSQLValue instances that specify the lower- and upper bound of a
   * range. A record falls into the range if {@code startKey <= recordKey <= endKey}.
   *
   * @param startKey AbstractSQLValue instance that specifies the lower bound (inclusive) of the
   *     range to lookup.
   * @param endKey AbstractSQLValue instance that specifies the upper bound (inclusive) of the range
   *     to lookup.
   * @return Iterator of RIDs of records stored in the indexed table and that fall into the
   *     specified range. Returns an empty iterator if no records fall into the range.
   */
  public abstract Iterator<RecordIdentifier> rangeLookup(T startKey, T endKey);

  /**
   * Indicate whether the index supports duplicate keys or not. Indexes used for PRIMARY/UNIQUE
   * constraints should return true
   *
   * @return false if duplicate keys are supported, true otherwise
   */
  public abstract boolean isUniqueIndex();

  /** Print the index for debugging purposes */
  public abstract void print();

  /**
   * Return reference to table that is indexed
   *
   * @return Reference to the indexed table
   */
  public AbstractTable getTable() {
    return this.table;
  }
}
