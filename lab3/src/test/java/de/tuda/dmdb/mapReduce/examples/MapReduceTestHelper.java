package de.tuda.dmdb.mapReduce.examples;

import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLDouble;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;
import java.util.List;
import org.junit.jupiter.api.Assertions;

public class MapReduceTestHelper {
  public static AbstractRecord getRecord(Integer i, String s) {
    AbstractRecord r = new Record(2);
    r.setValue(0, new SQLInteger(i));
    SQLVarchar v = new SQLVarchar(s, 200);
    r.setValue(1, v);
    return r;
  }

  public static AbstractRecord getRecord(String s, Integer i) {
    AbstractRecord r = new Record(2);
    SQLVarchar v = new SQLVarchar(s, 200);
    r.setValue(0, v);
    r.setValue(1, new SQLInteger(i));
    return r;
  }

  public static AbstractRecord getRecord(Integer i, Integer i2) {
    AbstractRecord r = new Record(2);
    r.setValue(0, new SQLInteger(i));
    r.setValue(1, new SQLInteger(i2));
    return r;
  }

  public static AbstractRecord getRecord(Integer i, Double i2) {
    AbstractRecord r = new Record(2);
    r.setValue(0, new SQLInteger(i));
    r.setValue(1, new SQLDouble(i2));
    return r;
  }

  public static void compareResultSet(
      List<AbstractRecord> expectedList, List<HeapTable> resultTables) {
    for (HeapTable ht : resultTables) {
      TableScan scan = new TableScan(ht);
      scan.open();
      AbstractRecord resultRec;
      while ((resultRec = scan.next()) != null) {
        // Removing the record from the expected List, since it was returned.
        // If the record was not expected, it cannot be removed and the message should be displayed.
        Assertions.assertTrue(
            expectedList.remove(resultRec), resultRec + " should not be part of the result set.");
      }
      scan.close();
    }
    // After removing all records previously, there should not be any left.
    Assertions.assertTrue(
        expectedList.isEmpty(), "We expected more entries to be in the result list");
  }
}
