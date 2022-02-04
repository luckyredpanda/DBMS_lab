package de.tuda.dmdb.operator;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.AbstractDynamicIndex;
import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.access.exercise.UniqueBPlusTree;
import de.tuda.dmdb.operator.exercise.RangeIndexScan;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import java.util.ArrayList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRangeIndexScan extends TestCase {

  /** Tests that RangeScan returns correct record */
  @Test
  public void testRangeScanNext() {
    AbstractRecord record1 = new Record(1);
    record1.setValue(0, new SQLInteger(1));
    AbstractTable table = new HeapTable(record1.clone());
    AbstractDynamicIndex<SQLInteger> index = new UniqueBPlusTree<SQLInteger>(table, 0, 1);
    index.insert(record1);

    // index.print();

    RangeIndexScan<SQLInteger> rangeScan =
        new RangeIndexScan<>(table, index, new SQLInteger(0), new SQLInteger(1));

    rangeScan.open();
    Assertions.assertEquals(record1, rangeScan.next());
    rangeScan.close();
  }

  /** Test range scan returns all records */
  @Test
  public void testRangeScanSimple() {
    final int NUM_RECORDS = 10;
    AbstractRecord record1 = new Record(1);
    record1.setValue(0, new SQLInteger(1));
    AbstractTable table = new HeapTable(record1.clone());
    AbstractDynamicIndex<SQLInteger> index = new UniqueBPlusTree<SQLInteger>(table, 0, 1);
    for (int i = 0; i < NUM_RECORDS; i++) {
      AbstractRecord record = record1.clone();
      record.setValue(0, new SQLInteger(i));
      index.insert(record);
    }

    // index.print();

    RangeIndexScan<SQLInteger> rangeScan =
        new RangeIndexScan<>(table, index, new SQLInteger(0), new SQLInteger(NUM_RECORDS));

    ArrayList<AbstractRecord> resultRecords = new ArrayList<>();
    rangeScan.open();
    AbstractRecord record = rangeScan.next();
    Assertions.assertNotNull(record);
    while (record != null) {
      resultRecords.add(record);
      record = rangeScan.next();
    }
    rangeScan.close();
    Assertions.assertEquals(NUM_RECORDS, resultRecords.size());
  }
}
