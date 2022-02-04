package de.tuda.dmdb.mapReduce.task;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.mapReduce.operator.MapReduceOperator;
import de.tuda.dmdb.mapReduce.operator.exercise.Reducer;
import de.tuda.dmdb.mapReduce.task.exercise.ReducerTask;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;
import java.util.*;
import java.util.Map.Entry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestReducerTask extends TestCase {

  public static class IntSumReducer
      extends Reducer<SQLVarchar, SQLInteger, SQLVarchar, SQLInteger> {

    private SQLInteger result = new SQLInteger();

    @Override
    public void reduce(SQLVarchar key, Iterable<SQLInteger> values, Queue<AbstractRecord> outList) {
      int sum = 0;
      for (SQLInteger val : values) {
        sum += val.getValue();
      }

      result.setValue(sum);

      AbstractRecord newRecord = MapReduceOperator.keyValueRecordPrototype.clone();
      newRecord.setValue(MapReduceOperator.KEY_COLUMN, (SQLVarchar) key);
      newRecord.setValue(MapReduceOperator.VALUE_COLUMN, result.clone());

      nextList.add(newRecord);
    }
  }

  @Test
  public void testReducerSimple() {
    String refString = "the quick brown the fox";
    AbstractRecord templateRecord = new Record(2);
    templateRecord.setValue(0, new SQLVarchar("key", 30));
    templateRecord.setValue(1, new SQLInteger(1));
    HeapTable table1 = new HeapTable(templateRecord);

    Map<String, Integer> expectedResultMap = new TreeMap<String, Integer>();
    String[] sortedStrings = refString.split(" ");
    Arrays.sort(sortedStrings);
    for (String key : sortedStrings) {
      AbstractRecord record = new Record(2);
      record.setValue(0, new SQLVarchar(key, 30));
      record.setValue(1, new SQLInteger(1));
      table1.insert(record);
      if (expectedResultMap.containsKey(key)) {
        expectedResultMap.put(key, expectedResultMap.get(key) + 1);
      } else {
        expectedResultMap.put(key, 1);
      }
    }

    HeapTable reducerResult = new HeapTable(templateRecord);

    ReducerTask thread = new ReducerTask(table1, reducerResult, IntSumReducer.class);
    thread.start();
    try {
      thread.join();
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.assertTrue(false);
    }

    List<AbstractRecord> actualResult = new ArrayList<AbstractRecord>();
    AbstractRecord rec;
    TableScan scan = new TableScan(reducerResult);
    scan.open();
    while ((rec = scan.next()) != null) {
      actualResult.add(rec);
    }
    scan.close();

    List<AbstractRecord> expectedResult = new ArrayList<AbstractRecord>();
    Iterator<Entry<String, Integer>> eIterator = expectedResultMap.entrySet().iterator();
    while (eIterator.hasNext()) {
      Map.Entry<String, Integer> mentry = (Map.Entry<String, Integer>) eIterator.next();
      AbstractRecord record = new Record(2);
      record.setValue(0, new SQLVarchar(mentry.getKey(), 30));
      record.setValue(1, new SQLInteger(mentry.getValue()));
      expectedResult.add(record);
    }

    Assertions.assertEquals(expectedResultMap.size(), actualResult.size());

    for (int i = 0; i < expectedResult.size(); i++) {
      AbstractRecord value1 = expectedResult.get(i);
      AbstractRecord value2 = actualResult.get(i);
      Assertions.assertEquals(value1, value2);
    }
  }
}
