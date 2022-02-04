package de.tuda.dmdb.access;

import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;

/**
 * Abstract class for an single dimensional unique index
 *
 * @author cbinnig
 * @param <T> type of key
 */
public abstract class AbstractUniqueIndex<T extends AbstractSQLValue> {

  protected AbstractTable table; // table to be indexed

  /**
   * Creates an index for a given table
   *
   * @param table the table over which the index is built
   */
  public AbstractUniqueIndex(AbstractTable table) {
    this.table = table;
  }

  /**
   * Returns table
   *
   * @return the table over which the index is built
   */
  public AbstractTable getTable() {
    return table;
  }

  /**
   * Inserts a record into the index and into its table
   *
   * @param record the record to be inserted
   * @return true if successful, false otherwise
   */
  public abstract boolean insert(AbstractRecord record);

  /**
   * Returns a record for a given key
   *
   * @param key the key to search an entry for
   * @return the record matching the key or null otherwise
   */
  public abstract AbstractRecord lookup(T key);

  /** Print the index for debugging */
  public abstract void print();
}
