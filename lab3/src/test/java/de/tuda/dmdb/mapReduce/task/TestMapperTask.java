package de.tuda.dmdb.mapReduce.task;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.mapReduce.operator.MapReduceOperator;
import de.tuda.dmdb.mapReduce.operator.exercise.Mapper;
import de.tuda.dmdb.mapReduce.task.exercise.MapperTask;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.StringTokenizer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMapperTask extends TestCase {

  public static class TokenizerMapper
      extends Mapper<SQLInteger, SQLVarchar, SQLVarchar, SQLInteger> {

    private static final int one = 1;
    private SQLVarchar word = new SQLVarchar(255);

    @Override
    public void map(SQLInteger key, SQLVarchar value, Queue<AbstractRecord> outList) {

      StringTokenizer itr = new StringTokenizer(value.getValue());
      while (itr.hasMoreTokens()) {

        word.setValue(itr.nextToken());

        AbstractRecord newRecord = MapReduceOperator.keyValueRecordPrototype.clone();
        newRecord.setValue(MapReduceOperator.KEY_COLUMN, word.clone());
        newRecord.setValue(MapReduceOperator.VALUE_COLUMN, new SQLInteger(one));

        outList.add(newRecord);
      }
    }
  }

  @Test
  public void testMapperSimple() {
    String refString = "the quick brown the fox";
    AbstractRecord record1 = new Record(2);
    record1.setValue(0, new SQLInteger(0));
    record1.setValue(1, new SQLVarchar(refString, 30));
    HeapTable table1 = new HeapTable(record1);
    table1.insert(record1);

    List<AbstractRecord> expectedResult = new ArrayList<AbstractRecord>();
    StringTokenizer itr = new StringTokenizer(refString);
    while (itr.hasMoreTokens()) {
      AbstractRecord record = new Record(2);
      record.setValue(0, new SQLVarchar(itr.nextToken(), 30));
      record.setValue(1, new SQLInteger(1));
      expectedResult.add(record);
    }

    AbstractRecord resultRecord = new Record(2);
    resultRecord.setValue(0, new SQLVarchar("key", 30));
    resultRecord.setValue(1, new SQLInteger(1));

    HeapTable mapperResult = new HeapTable(resultRecord);

    MapperTask thread = new MapperTask(table1, mapperResult, TokenizerMapper.class);
    thread.start();
    try {
      thread.join();
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.assertTrue(false);
    }

    List<AbstractRecord> actualResult = new ArrayList<AbstractRecord>();
    AbstractRecord rec;
    TableScan scan = new TableScan(mapperResult);
    scan.open();
    while ((rec = scan.next()) != null) {
      actualResult.add(rec);
    }
    scan.close();
    Assertions.assertEquals(expectedResult.size(), actualResult.size());
    for (int i = 0; i < expectedResult.size(); i++) {
      AbstractRecord value1 = expectedResult.get(i);
      AbstractRecord value2 = actualResult.get(i);
      Assertions.assertEquals(value1, value2);
    }
  }
}
