package de.tuda.dmdb.operator;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.AbstractDynamicIndex;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.access.exercise.UniqueBPlusTree;
import de.tuda.dmdb.operator.exercise.IndexScan;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIndexScan extends TestCase {

  /** Tests that IndexScan returns correct record */
  @Test
  public void testIndexScanNext() {
    AbstractRecord r = new Record(2);
    r.setValue(0, new SQLInteger(1));
    r.setValue(1, new SQLInteger(2));
    HeapTable ht = new HeapTable(r);
    AbstractDynamicIndex<SQLInteger> index = new UniqueBPlusTree<>(ht, 1);
    IndexScan<SQLInteger> indexScan = new IndexScan<>(ht, index, new SQLInteger(2));
    indexScan.open();
    Assertions.assertNull(indexScan.next());
    indexScan.close();
    ht.insert(r);
    index.insert(r);

    indexScan.open();
    Assertions.assertEquals(r, indexScan.next());
    indexScan.close();
  }
}
