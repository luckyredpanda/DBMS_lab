package de.tuda.dmdb.access;

import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;

/**
 * This class adds support for updating indexes.
 *
 * @author cbinnig
 * @param <T> type of key
 */
public abstract class AbstractDynamicIndex<T extends AbstractSQLValue> extends AbstractIndex<T> {

  /**
   * Creates an index for a given table
   *
   * @param table the table over which the index is built
   */
  protected AbstractDynamicIndex(AbstractTable table) {
    super(table);
  }

  /**
   * Inserts a record into the index and into its table
   *
   * @param record the record to be inserted
   * @return true if successful, false otherwise
   */
  public abstract boolean insert(AbstractRecord record);
}
