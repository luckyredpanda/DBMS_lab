package de.tuda.dmdb.access;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.access.exercise.NaiveBitmapIndex;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestNaiveBitmapIndex extends TestCase {

  /** Insert records with different values should create separate bitmaps */
  @Test
  public void testBulkLoadSimple() {
    int size = 4;
    int keyColumnNumber = 0;
    AbstractRecord record = new Record(2);
    record.setValue(0, new SQLInteger());
    record.setValue(1, new SQLVarchar(100));

    AbstractTable table = new HeapTable(record.clone());

    // insert
    for (int i = 0; i < size; i++) {
      record.setValue(0, new SQLInteger(i));
      record.setValue(1, new SQLVarchar("Hello" + i, 100));
      table.insert(record);
    }

    AbstractBitmapIndex<SQLInteger> index =
        new NaiveBitmapIndex<SQLInteger>(table, keyColumnNumber);
    // index.print();

    // bitmaps size should be size, as we have different values
    Assertions.assertEquals(size, index.getBitMaps().size());
    for (SQLInteger key : index.getBitMaps().keySet()) {
      BitSet bitmap = index.getBitMaps().get(key);
      Assertions.assertEquals(1, bitmap.cardinality());
    }
  }

  /** Insert four records and reads them again using a SQLInteger index */
  @Test
  public void testRangeLookupSimple() {
    AbstractRecord record1 = new Record(2);
    record1.setValue(0, new SQLInteger(1));
    record1.setValue(1, new SQLVarchar("Hello111", 10));

    AbstractRecord record2 = new Record(2);
    record2.setValue(0, new SQLInteger(2));
    record2.setValue(1, new SQLVarchar("Hello112", 10));

    AbstractRecord record3 = new Record(2);
    record3.setValue(0, new SQLInteger(3));
    record3.setValue(1, new SQLVarchar("Hello113", 10));

    AbstractRecord record4 = new Record(2);
    record4.setValue(0, new SQLInteger(4));
    record4.setValue(1, new SQLVarchar("Hello114", 10));

    AbstractTable table = new HeapTable(record1.clone());

    table.insert(record1);
    table.insert(record2);
    table.insert(record3);
    table.insert(record3);

    AbstractBitmapIndex<SQLInteger> index = new NaiveBitmapIndex<SQLInteger>(table, 0);
    // index.print();

    {
      List<AbstractRecord> result = new ArrayList<>();
      Iterator<RecordIdentifier> resultIterator =
          index.rangeLookup((SQLInteger) record1.getValue(0), (SQLInteger) record2.getValue(0));
      while (resultIterator.hasNext()) {
        AbstractRecord record = table.lookup(resultIterator.next());
        result.add(record);
      }
      Assertions.assertEquals(2, result.size());
      Assertions.assertEquals(record1, result.get(0));
      Assertions.assertEquals(record2, result.get(1));
    }

    {
      List<AbstractRecord> result = new ArrayList<>();
      Iterator<RecordIdentifier> resultIterator =
          index.rangeLookup((SQLInteger) record3.getValue(0), (SQLInteger) record4.getValue(0));
      while (resultIterator.hasNext()) {
        AbstractRecord record = table.lookup(resultIterator.next());
        result.add(record);
      }

      Assertions.assertEquals(2, result.size());
      Assertions.assertEquals(record3, result.get(0));
      Assertions.assertEquals(record3, result.get(1));
    }
  }

  /** Insert four records and reads them again using a SQLInteger index */
  @Test
  public void testKeyLookupSimple() {
    AbstractRecord record1 = new Record(2);
    record1.setValue(0, new SQLInteger(1));
    record1.setValue(1, new SQLVarchar("Hello111", 10));

    AbstractRecord record2 = new Record(2);
    record2.setValue(0, new SQLInteger(2));
    record2.setValue(1, new SQLVarchar("Hello112", 10));

    AbstractRecord record3 = new Record(2);
    record3.setValue(0, new SQLInteger(3));
    record3.setValue(1, new SQLVarchar("Hello113", 10));

    AbstractRecord record4 = new Record(2);
    record4.setValue(0, new SQLInteger(4));
    record4.setValue(1, new SQLVarchar("Hello114", 10));

    AbstractTable table = new HeapTable(record1.clone());

    table.insert(record1);
    table.insert(record2);
    table.insert(record3);
    table.insert(record4);

    AbstractBitmapIndex<SQLInteger> index = new NaiveBitmapIndex<SQLInteger>(table, 0);
    // index.print();

    AbstractRecord record1Cmp = table.lookup(index.lookup((SQLInteger) record1.getValue(0)).next());
    Assertions.assertEquals(record1, record1Cmp);

    AbstractRecord record2Cmp = table.lookup(index.lookup((SQLInteger) record2.getValue(0)).next());
    Assertions.assertEquals(record2, record2Cmp);

    AbstractRecord record3Cmp = table.lookup(index.lookup((SQLInteger) record3.getValue(0)).next());
    Assertions.assertEquals(record3, record3Cmp);
  }

  /** Insert three records and reads them again using a SQLVarchar index */
  @Test
  public void testIndexSimple2() {
    AbstractRecord record1 = new Record(2);
    record1.setValue(0, new SQLInteger(1));
    record1.setValue(1, new SQLVarchar("Hello111", 10));

    AbstractRecord record2 = new Record(2);
    record2.setValue(0, new SQLInteger(2));
    record2.setValue(1, new SQLVarchar("Hello112", 10));

    AbstractRecord record3 = new Record(2);
    record3.setValue(0, new SQLInteger(3));
    record3.setValue(1, new SQLVarchar("Hello113", 10));

    AbstractTable table = new HeapTable(record1.clone());

    table.insert(record1);
    table.insert(record2);
    table.insert(record3);
    // index.print();

    AbstractBitmapIndex<SQLVarchar> index = new NaiveBitmapIndex<SQLVarchar>(table, 1);

    AbstractRecord record1Cmp = table.lookup(index.lookup((SQLVarchar) record1.getValue(1)).next());
    Assertions.assertEquals(record1, record1Cmp);

    AbstractRecord record2Cmp = table.lookup(index.lookup((SQLVarchar) record2.getValue(1)).next());
    Assertions.assertEquals(record2, record2Cmp);

    AbstractRecord record3Cmp = table.lookup(index.lookup((SQLVarchar) record3.getValue(1)).next());
    Assertions.assertEquals(record3, record3Cmp);
  }
}
