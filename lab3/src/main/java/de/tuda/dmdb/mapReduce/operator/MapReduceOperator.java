package de.tuda.dmdb.mapReduce.operator;

import de.tuda.dmdb.operator.Operator;
import de.tuda.dmdb.operator.UnaryOperator;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import java.util.Queue;

/**
 * Base class to implement map+reduce operators as iterators Defines Constants for the data format
 * used (KeyValue-records and tables) Defines the "Context" to which mapper and reducer can write
 * their output
 *
 * @author melhindi
 */
public abstract class MapReduceOperator extends UnaryOperator {

  public static final int KEY_COLUMN =
      0; // key will be stored in this column of a record, ie. record.getValue(KEY_COLUMN) will
  // return the key
  public static final int VALUE_COLUMN =
      1; // value will be stored in this column of a record, ie. record.getValue(VALUE_COLUMN) will
  // return the value
  public static final int NUM_RECORD_COLUMNS = 2;
  public static AbstractRecord keyValueRecordPrototype =
      new Record(MapReduceOperator.NUM_RECORD_COLUMNS);
  /**
   * Used to write results of mapper/reducers functions, think of it as a cache Needs to be
   * initialized by the implementing class
   */
  protected Queue<AbstractRecord> nextList;

  /**
   * Provide default constructor so that mapper/reducer-operators can be created with
   * class.newInstance()
   */
  public MapReduceOperator() {
    super(null);
  }

  /**
   * Default constructor for UnaryOperator Use it to set the child on construction
   *
   * @param child - child operator
   */
  public MapReduceOperator(Operator child) {
    super(child);
  }
}
