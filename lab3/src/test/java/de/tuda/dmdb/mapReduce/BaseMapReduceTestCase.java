package de.tuda.dmdb.mapReduce;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import org.junit.jupiter.api.Assertions;

public abstract class BaseMapReduceTestCase extends TestCase {

  protected Comparator<AbstractRecord> recordComparator =
      new Comparator<AbstractRecord>() {
        public int compare(AbstractRecord record1, AbstractRecord record2) {

          AbstractSQLValue value1 = record1.getValue(0);
          AbstractSQLValue value2 = record2.getValue(0);

          return value1.compareTo(value2);
        }
      };

  protected void initPartitionsAndExpectedResult(
      String[] partitionStrings,
      int stringLength,
      List<HeapTable> partitions,
      List<AbstractRecord> expectedResult) {
    HashMap<String, Integer> expectedResultMap = new HashMap<>();
    for (int i = 0; i < partitionStrings.length; i++) {
      String string = partitionStrings[i];
      AbstractRecord record = new Record(2);
      record.setValue(0, new SQLInteger(i));
      record.setValue(1, new SQLVarchar(string, stringLength));
      HeapTable table = new HeapTable(record);
      table.insert(record);
      partitions.add(table);

      // create expected resultMap
      StringTokenizer stringTokenizer = new StringTokenizer(string);
      while (stringTokenizer.hasMoreTokens()) {
        String key = stringTokenizer.nextToken();
        if (expectedResultMap.containsKey(key)) {
          expectedResultMap.put(key, expectedResultMap.get(key) + 1);
        } else {
          expectedResultMap.put(key, 1);
        }
      }
    }
    Assertions.assertEquals(partitionStrings.length, partitions.size());

    Iterator<Entry<String, Integer>> eIterator = expectedResultMap.entrySet().iterator();
    while (eIterator.hasNext()) {
      Map.Entry<String, Integer> mentry = (Map.Entry<String, Integer>) eIterator.next();
      AbstractRecord record = new Record(2);
      record.setValue(0, new SQLVarchar(mentry.getKey(), stringLength));
      record.setValue(1, new SQLInteger(mentry.getValue()));
      expectedResult.add(record);
    }
  }
}
