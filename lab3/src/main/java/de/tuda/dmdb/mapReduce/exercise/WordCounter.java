package de.tuda.dmdb.mapReduce.exercise;

import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.catalog.objects.Table;
import de.tuda.dmdb.mapReduce.Configuration;
import de.tuda.dmdb.mapReduce.Job;
import de.tuda.dmdb.mapReduce.MapReduceExecutor;
import de.tuda.dmdb.mapReduce.MultiPhaseExecutor;
import de.tuda.dmdb.mapReduce.operator.MapReduceOperator;
import de.tuda.dmdb.mapReduce.operator.exercise.Mapper;
import de.tuda.dmdb.mapReduce.operator.exercise.Reducer;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;
import de.tuda.dmdb.mapReduce.task.SinglePhaseTaskBase;

import java.util.Queue;
import java.util.StringTokenizer;

/**
 * Counts how often words each word was used. Input is textId,text. Output is word, numOccurrences.
 */
public class WordCounter implements BaseMapReduceExercise {
  // TODO implement Mapper and Reducer using public static classes
  Mapper<SQLInteger, SQLVarchar, SQLVarchar, SQLInteger> mapper = new Mapper<>();
  Reducer<SQLVarchar, SQLInteger, SQLVarchar, SQLInteger> reducer = new Reducer<>();

  public static class TokenizerMapper
          extends Mapper<SQLInteger, SQLVarchar, SQLVarchar, SQLInteger> {

    private SQLVarchar word = new SQLVarchar(255);
    private SQLInteger res=new SQLInteger(1);

    @Override
    public void map(SQLInteger key, SQLVarchar value, Queue<AbstractRecord> outList) {
      StringTokenizer itr = new StringTokenizer(value.getValue());
      while (itr.hasMoreTokens()) {
        word.setValue(itr.nextToken());
        AbstractRecord newRecord = MapReduceOperator.keyValueRecordPrototype.clone();
        newRecord.setValue(MapReduceOperator.KEY_COLUMN, word.clone());
        newRecord.setValue(MapReduceOperator.VALUE_COLUMN, res);
        outList.add(newRecord);
      }
    }
  }

  public static class IntSumReducer
          extends Reducer<SQLVarchar, SQLInteger, SQLVarchar, SQLInteger> {

    private SQLInteger result = new SQLInteger(0);


    @Override
    public void reduce(SQLVarchar key, Iterable<SQLInteger> values, Queue<AbstractRecord> outList) {
      System.out.println("key"+key);
      System.out.println("value"+values);
      System.out.println("outlist"+outList);
      int sum = 0;
      //System.out.println("val"+values.iterator());

      for (SQLInteger val : values) {
        System.out.println("val"+val.getValue());
        sum += val.getValue();
      }
      result.setValue(sum);
      System.out.println("sum"+sum);

      AbstractRecord newRecord = MapReduceOperator.keyValueRecordPrototype.clone();
      newRecord.setValue(MapReduceOperator.KEY_COLUMN, (SQLVarchar) key);
      newRecord.setValue(MapReduceOperator.VALUE_COLUMN, result.clone());
      outList.add(newRecord);
    }
  }

  @Override
  public Job createJob(Configuration config) {
    // TODO implement this method
    int stringLength = 100;
    AbstractRecord outputPrototype = new Record(2);
    outputPrototype.setValue(0, new SQLVarchar(stringLength));
    outputPrototype.setValue(1, new SQLInteger(0));
    Job job = Job.getInstance(config, "word count");
    System.out.println("job"+job.getInputs());
    job.setMapperClass(TokenizerMapper.class);
    System.out.println(TokenizerMapper.keyValueRecordPrototype);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputPrototype(outputPrototype);
    return job;
//    return null;
  }
}
