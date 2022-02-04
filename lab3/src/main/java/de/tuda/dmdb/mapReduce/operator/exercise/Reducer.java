package de.tuda.dmdb.mapReduce.operator.exercise;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import de.tuda.dmdb.mapReduce.operator.ReducerBase;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;

/**
 * similar to
 * https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/Reducer.java
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
public class Reducer<
        KEYIN extends AbstractSQLValue,
        VALUEIN extends AbstractSQLValue,
        KEYOUT extends AbstractSQLValue,
        VALUEOUT extends AbstractSQLValue>
        extends ReducerBase<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  @Override
  public void open() {
    // TODO: implement this method
    // make sure to initialize ALL (inherited) member variables
    this.child.open();
    this.nextList = new LinkedList<>();
    this.lastRecord = this.child.next();
  }

  @Override
  @SuppressWarnings("unchecked")
  public AbstractRecord next() {
    // TODO: implement this method
    // this method has to prepare the input to the reduce function
    // it also returns the result of the reduce function as next
    // Implement the grouping of mapper-outputs here
    // You can assume that the input to the reducer is sorted. This makes the grouping operation
    // easier. Keep this in mind if you write your own tests (make sure that input to reducer is
    // sorted)
    while (this.lastRecord != null) {
      KEYIN key = (KEYIN)this.lastRecord.getValue(this.KEY_COLUMN);
      VALUEIN value = (VALUEIN)this.lastRecord.getValue(this.VALUE_COLUMN);
      List<VALUEIN> reduce_value = new ArrayList<>();  // a list of values with the same key
      reduce_value.add(value);
      // grouping the sorted input records

      while ((this.lastRecord = this.child.next())!= null) {
        KEYIN nextKey = (KEYIN) this.lastRecord.getValue(this.KEY_COLUMN);
        if (nextKey.compareTo(key) == 0) {  // lastRecord still belongs to the current group
          value = (VALUEIN)this.lastRecord.getValue(this.VALUE_COLUMN);
          reduce_value.add(value);
        }
        else  // find a new group
          break;
      }
      // execute reduce operator on a group
      this.reduce(key, reduce_value, this.nextList);
      return this.nextList.poll();
    }
    AbstractRecord nextRec;
    nextRec = this.nextList.poll();
    while (nextRec != null)
      return nextRec;
    return null;
//    return nextRec;
  }

  @Override
  public void close() {
    // TODO: implement this method
    // reverse what was done in open()
    this.child.close();
    this.nextList = null;
    this.lastRecord = null;
  }
}
