package de.tuda.dmdb.access;

import static org.mockito.Mockito.mockStatic;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.access.exercise.Leaf;
import de.tuda.dmdb.access.exercise.UniqueBPlusTree;
import de.tuda.dmdb.buffer.DummyBufferManager;
import de.tuda.dmdb.buffer.exercise.BufferManager;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestLeaf extends TestCase {

  DummyBufferManager dummyBufferManager = new DummyBufferManager();

  @BeforeEach
  public void resetBufferManager() {
    dummyBufferManager = new DummyBufferManager();
  }

  /** Test that records are inserted into HeapTable */
  @Test
  public void testLeafInsertsIntoTable() {
    try (MockedStatic mocked = mockStatic(BufferManager.class)) {
      mocked.when(BufferManager::getInstance).thenReturn(dummyBufferManager);

      AbstractRecord record1 = new Record(2);
      record1.setValue(0, new SQLInteger(1));
      record1.setValue(1, new SQLVarchar("Hello111", 10));

      AbstractRecord record2 = new Record(2);
      record2.setValue(0, new SQLInteger(2));
      record2.setValue(1, new SQLVarchar("Hello112", 10));

      AbstractTable table = new HeapTable(record1.clone());

      UniqueBPlusTree<SQLInteger> index = new UniqueBPlusTree<SQLInteger>(table, 0, 1);
      Leaf<SQLInteger> leaf = new Leaf<SQLInteger>(index);
      Assertions.assertEquals(0, table.getRecordCount());
      Assertions.assertTrue(leaf.insert((SQLInteger) record1.getValue(0), record1));
      Assertions.assertEquals(1, table.getRecordCount());
      // inserting same record again should return false
      Assertions.assertFalse(leaf.insert((SQLInteger) record1.getValue(0), record1));
      Assertions.assertEquals(1, table.getRecordCount());
      // inserting different record should work
      Assertions.assertTrue(leaf.insert((SQLInteger) record2.getValue(0), record2));
      Assertions.assertEquals(2, table.getRecordCount());
    }
  }

  /** Insert lookup returns correct record or null */
  @Test
  public void testLeafLookup() {
    try (MockedStatic mocked = mockStatic(BufferManager.class)) {
      mocked.when(BufferManager::getInstance).thenReturn(dummyBufferManager);

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

      UniqueBPlusTree<SQLInteger> index = new UniqueBPlusTree<SQLInteger>(table, 0, 1);
      Leaf<SQLInteger> leaf = new Leaf<SQLInteger>(index);

      Assertions.assertTrue(leaf.insert((SQLInteger) record1.getValue(0), record1));
      Assertions.assertTrue(leaf.insert((SQLInteger) record2.getValue(0), record2));
      Assertions.assertEquals(record1, leaf.lookup((SQLInteger) record1.getValue(0)));
      Assertions.assertNull(leaf.lookup((SQLInteger) record3.getValue(0)));
    }
  }
}
