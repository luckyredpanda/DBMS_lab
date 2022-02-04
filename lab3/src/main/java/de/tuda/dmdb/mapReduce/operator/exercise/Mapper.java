package de.tuda.dmdb.mapReduce.operator.exercise;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

import de.tuda.dmdb.mapReduce.operator.MapperBase;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;

/**
 * similar to:
 * https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/Mapper.java
 *
 * <p>Maps input key/value pairs to a set of intermediate key/value pairs.
 *
 * <p>Maps are the individual tasks which transform input records into a intermediate records. A
 * given input pair may map to zero or many output pairs. Output pairs are normally written to the
 * nextList member
 *
 * <p>The class implements the iterator model and works on records. Records are read to retrieve the
 * KEY and VALUE, which are passed to the map() function The map function performs the defined
 * mapping and writes the new output to the passed in record List Use the member nextList as input
 * to the map() function to cache produced output across multiple next() calls.
 *
 * @author melhindi
 * @param <KEYIN> SQLValue type of the input key
 * @param <VALUEIN> SQLValue type of the input value
 * @param <KEYOUT> SQLValue type of the output key
 * @param <VALUEOUT> SQLValue type of the output value
 */
public class Mapper<
        KEYIN extends AbstractSQLValue,
        VALUEIN extends AbstractSQLValue,
        KEYOUT extends AbstractSQLValue,
        VALUEOUT extends AbstractSQLValue>
        extends MapperBase<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  @Override
  public void open() {
    // TODO: implement this method
    // make sure to initialize ALL (inherited) member variables
    this.child.open();
    this.nextList = new LinkedList<>();


  }

  @Override
  @SuppressWarnings("unchecked")
  public AbstractRecord next() {
    // TODO: implement this method
    // this method has to retrieve and prepare the input to the map function
    // it also returns the result of the map function as next
    // keep in mind that a mapper potentially maps one AbstractRecord to multiple other
    // AbstractRecords
    AbstractRecord nextRec;
    nextRec = this.child.next();
    while (nextRec != null) {
      KEYIN key = (KEYIN)nextRec.getValue(this.KEY_COLUMN);
      VALUEIN value = (VALUEIN)nextRec.getValue(this.VALUE_COLUMN);
      this.map(key, value, this.nextList);
      return this.nextList.poll();
    }
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
  }
}
