package de.tuda.dmdb.operator;

import de.tuda.dmdb.storage.AbstractRecord;

/**
 * Implementation of the Vulcano-Iterator interface
 *
 * @author cbinning
 */
public abstract class Operator {
  /**
   * Initialize/prepare the operator. Usually by initializing child/children operators and setting
   * member variables
   */
  public abstract void open();

  /**
   * Return the next record processed by the operator to the caller.
   *
   * @return - the next record, or NULL if there are no records left
   * @throws NullPointerException if next was called although operation has not been initialized
   *     through open()
   */
  public abstract AbstractRecord next();

  /**
   * Uninitialize operator and create a clean state for a new open() call. Usually reverses what was
   * done in open()
   */
  public abstract void close();
}
