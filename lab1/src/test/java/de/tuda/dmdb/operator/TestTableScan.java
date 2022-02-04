package de.tuda.dmdb.operator;

import static org.mockito.Mockito.mockStatic;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.buffer.DummyBufferManager;
import de.tuda.dmdb.buffer.exercise.BufferManager;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestTableScan extends TestCase {

  DummyBufferManager dummyBufferManager = new DummyBufferManager();

  @BeforeEach
  public void resetBufferManager() {
    dummyBufferManager = new DummyBufferManager();
  }

  /** Tests that TableScan returns records with only the selected attributes */
  @Test
  public void testTableScanNext() {
    try (MockedStatic mocked = mockStatic(BufferManager.class)) {
      mocked.when(BufferManager::getInstance).thenReturn(dummyBufferManager);

      AbstractRecord r = new Record(2);
      r.setValue(0, new SQLInteger(1));
      r.setValue(1, new SQLInteger(2));
      HeapTable ht = new HeapTable(r);
      TableScan ts = new TableScan(ht);
      ts.open();
      Assertions.assertNull(ts.next());
      ts.close();
      ht.insert(r);

      ts.open();
      Assertions.assertEquals(r, ts.next());
      ts.close();
    }
  }
}
