package de.tuda.dmdb.mapReduce.exercise;

import de.tuda.dmdb.mapReduce.Configuration;
import de.tuda.dmdb.mapReduce.Job;
import de.tuda.dmdb.mapReduce.operator.MapReduceOperator;
import de.tuda.dmdb.mapReduce.operator.exercise.Mapper;
import de.tuda.dmdb.mapReduce.operator.exercise.Reducer;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;

import java.util.Queue;
import java.util.StringTokenizer;
import java.util.concurrent.Executor;

/**
 * Implements a dot product. Input Data are two vectors, such that each column is one vector. E.g
 * for (1 2) dot (5 3): <code>
 *   KEY VALUE
 *   1 5
 *   2 3
 * </code> The result should be a single value with key 1.
 */
public class DotProduct implements BaseMapReduceExercise {
  // TODO implement Mapper and Reducer using public static classes
  public static Mapper<SQLInteger,SQLInteger,SQLInteger,SQLInteger> mapper = new Mapper<>();
  public static Reducer<SQLInteger,SQLInteger,SQLInteger,SQLInteger> reducer = new Reducer<>();

  public static class multiple
          extends Mapper<SQLInteger, SQLInteger, SQLInteger, SQLInteger> {

    private static final int one = 1;
    private SQLInteger word = new SQLInteger(1);

    @Override
    public void map(SQLInteger key, SQLInteger value, Queue<AbstractRecord> outList) {

      AbstractRecord newRecord = MapReduceOperator.keyValueRecordPrototype.clone();
      newRecord.setValue(MapReduceOperator.KEY_COLUMN, word.clone());
      newRecord.setValue(MapReduceOperator.VALUE_COLUMN, new SQLInteger(key.getValue()*value.getValue()));
      outList.add(newRecord);
    }
  }

  public static class add
          extends Reducer<SQLInteger, SQLInteger, SQLInteger, SQLInteger> {

    private SQLInteger result = new SQLInteger();

    @Override
    public void reduce(SQLInteger key, Iterable<SQLInteger> values, Queue<AbstractRecord> outList) {
      int sum = 0;
      for (SQLInteger val : values) {
        sum += val.getValue();
      }

      result.setValue(sum);

      AbstractRecord newRecord = MapReduceOperator.keyValueRecordPrototype.clone();
      newRecord.setValue(MapReduceOperator.KEY_COLUMN, key);
      newRecord.setValue(MapReduceOperator.VALUE_COLUMN, result.clone());
      outList.add(newRecord);
    }
  }

  @Override
  public Job createJob(Configuration config) {
    // TODO implement this method
    AbstractRecord outputPrototype = new Record(2);
    outputPrototype.setValue(0, new SQLInteger(0));
    outputPrototype.setValue(1, new SQLInteger(0));
    Job job = Job.getInstance(config, "Dot Product");
    job.setMapperClass(multiple.class);
    job.setReducerClass(add.class);
    job.setOutputPrototype(outputPrototype);
    return job;
//    return null;
  }
}
