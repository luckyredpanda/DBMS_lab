package de.tuda.dmdb.mapReduce.exercise;

import de.tuda.dmdb.mapReduce.Configuration;
import de.tuda.dmdb.mapReduce.Job;
import de.tuda.dmdb.mapReduce.operator.MapReduceOperator;
import de.tuda.dmdb.mapReduce.operator.exercise.Mapper;
import de.tuda.dmdb.mapReduce.operator.exercise.Reducer;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLDouble;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;

import java.util.Queue;

/**
 * Performs the query "SELECT userId,AVG(sessionLength) FROM user GROUP BY userId". The input table
 * is user with key=userId, value=sessionLength. You should output userId,AVG(sessionLength).
 */
public class UserAverageSessionLength implements BaseMapReduceExercise {
  // TODO implement Mapper and Reducer using public static classes
  public static Mapper<SQLInteger,SQLDouble,SQLInteger,SQLDouble> mapper = new Mapper<>();
  public static Reducer<SQLInteger,SQLDouble,SQLInteger, SQLDouble> reducer = new Reducer<>();

  public static class mapp
          extends Mapper<SQLInteger, SQLDouble, SQLInteger, SQLDouble> {

    //    private static final int one = 1;
    private SQLInteger word = new SQLInteger();

    @Override
    public void map(SQLInteger key, SQLDouble value, Queue<AbstractRecord> outList) {

      word.setValue(key.getValue());
      AbstractRecord newRecord = MapReduceOperator.keyValueRecordPrototype.clone();
      newRecord.setValue(MapReduceOperator.KEY_COLUMN, word.clone());
      newRecord.setValue(MapReduceOperator.VALUE_COLUMN, value);
      outList.add(newRecord);
    }
  }

  public static class add
          extends Reducer<SQLInteger, SQLDouble, SQLInteger, SQLDouble> {

    private SQLDouble result = new SQLDouble();

    @Override
    public void reduce(SQLInteger key, Iterable<SQLDouble> values, Queue<AbstractRecord> outList) {
      double sum = 0.0;
      int n = 0;
      for (SQLDouble val : values) {
        sum += val.getValue();
        n ++;
      }
      double avg = sum/n;

      result.setValue(avg);

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
    outputPrototype.setValue(1, new SQLDouble(0.0));
    Job job = Job.getInstance(config, "AVG");
    job.setMapperClass(mapp.class);
    job.setReducerClass(add.class);
    job.setOutputPrototype(outputPrototype);
    return job;
//    return null;
  }
}
