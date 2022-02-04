package de.tuda.dmdb.mapReduce.operator;

import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import java.util.Queue;

/**
 * similar to
 * https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/Reducer.java
 * Base Class for implementing the Reducer-Operator Hides complexity from students and provides
 * helper methods
 *
 * <p>Reduces a set of intermediate values which share a key to a smaller set of values.
 *
 * <p>A Reducer performs three primary tasks: 1) Read sorted input from child 2) Group input from
 * child (ie., prepare input to reduce function) 3) Invoke the reduce() method on the prepared input
 * to generate new output pairs
 *
 * <p>The output of the <code>Reducer</code> is <b>not re-sorted</b>.
 *
 * @author melhindi
 * @param <KEYIN> SQLValue type of the input key
 * @param <VALUEIN> SQLValue type of the input value
 * @param <KEYOUT> SQLValue type of the output key
 * @param <VALUEOUT> SQLValue type of the output value
 */
public abstract class ReducerBase<
        KEYIN extends AbstractSQLValue,
        VALUEIN extends AbstractSQLValue,
        KEYOUT extends AbstractSQLValue,
        VALUEOUT extends AbstractSQLValue>
    extends MapReduceOperator {
  protected AbstractRecord lastRecord;

  /**
   * This method is called once for each key. Most applications will define their reduce class by
   * overriding this method. The default implementation is an identity function.
   *
   * @param key The key that will be grouped on
   * @param values All values belonging to that key
   * @param nextList reference to queue to which the output will be written
   */
  @SuppressWarnings("unchecked")
  protected void reduce(KEYIN key, Iterable<VALUEIN> values, Queue<AbstractRecord> nextList) {
    for (VALUEIN value : values) {
      AbstractRecord newRecord = MapReduceOperator.keyValueRecordPrototype.clone();
      newRecord.setValue(MapReduceOperator.KEY_COLUMN, (KEYOUT) key);
      newRecord.setValue(MapReduceOperator.VALUE_COLUMN, (VALUEOUT) value);

      nextList.add(newRecord);
    }
  }
}
