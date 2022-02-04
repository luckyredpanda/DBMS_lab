package de.tuda.dmdb.access;

import static org.mockito.Mockito.mockStatic;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.access.exercise.Leaf;
import de.tuda.dmdb.access.exercise.Node;
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

public class TestNode extends TestCase {

  DummyBufferManager dummyBufferManager = new DummyBufferManager();

  @BeforeEach
  public void resetBufferManager() {
    dummyBufferManager = new DummyBufferManager();
  }

  /** Test that record is inserted into heaptable */
  @Test
  public void testNodeSplitsChild() {
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

      AbstractRecord record4 = new Record(2);
      record4.setValue(0, new SQLInteger(4));
      record4.setValue(1, new SQLVarchar("Hello114", 10));

      AbstractRecord record5 = new Record(2);
      record5.setValue(0, new SQLInteger(5));
      record5.setValue(1, new SQLVarchar("Hello115", 10));

      AbstractTable table = new HeapTable(record1.clone());

      UniqueBPlusTree<SQLInteger> index = new UniqueBPlusTree<SQLInteger>(table, 0, 1);
      Leaf<SQLInteger> leaf = new Leaf<SQLInteger>(index);
      Assertions.assertTrue(leaf.insert((SQLInteger) record2.getValue(0), record2));
      Assertions.assertTrue(leaf.insert((SQLInteger) record3.getValue(0), record3));
      Assertions.assertTrue(leaf.insert((SQLInteger) record4.getValue(0), record4));

      AbstractIndexElement<SQLInteger> indexElement1 = leaf.createInstance();
      AbstractIndexElement<SQLInteger> indexElement2 = leaf.createInstance();
      leaf.split(indexElement1, indexElement2);

      // create new (inner) node
      Node<SQLInteger> root = new Node<SQLInteger>(index);
      index.setRoot(root);
      AbstractRecord nodeRecord = index.getNodeRecPrototype().clone();
      nodeRecord.setValue(UniqueBPlusTreeBase.KEY_POS, indexElement1.getMaxKey());
      nodeRecord.setValue(
          UniqueBPlusTreeBase.PAGE_POS, new SQLInteger(indexElement1.getPageNumber()));
      root.getIndexPage().insert(nodeRecord);
      nodeRecord.setValue(UniqueBPlusTreeBase.KEY_POS, indexElement2.getMaxKey());
      nodeRecord.setValue(
          UniqueBPlusTreeBase.PAGE_POS, new SQLInteger(indexElement2.getPageNumber()));
      root.getIndexPage().insert(nodeRecord);

      // Current tree state:
      // 		2|(4)
      //	   /   \
      //   2|    3|4

      // insert new record
      Assertions.assertTrue(index.insert(record1));

      // Current tree state:
      // 		2|(4)
      //	   /   \
      //   1|2   3|4

      // insert should split child, create overflow and add a new separator key to root:
      Assertions.assertTrue(index.insert(record5));

      // Final tree state:
      // 		2|3|(5)
      //	   /   \   \
      //   1|2   3|  4|5

      // root should not be overfull (i.e. more than maxFillGrade + 1)
      Assertions.assertFalse(root.isOverfull());
      Assertions.assertEquals(3, root.getIndexPage().getNumRecords());
    }
  }

  /** Test lookup returns correct record or null */
  @Test
  public void testNodeLookup() {
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

      AbstractRecord record4 = new Record(2);
      record4.setValue(0, new SQLInteger(4));
      record4.setValue(1, new SQLVarchar("Hello114", 10));

      AbstractTable table = new HeapTable(record1.clone());

      UniqueBPlusTree<SQLInteger> index = new UniqueBPlusTree<SQLInteger>(table, 0, 1);
      Leaf<SQLInteger> leaf = new Leaf<SQLInteger>(index);
      Assertions.assertTrue(leaf.insert((SQLInteger) record2.getValue(0), record2));
      Assertions.assertTrue(leaf.insert((SQLInteger) record3.getValue(0), record3));
      Assertions.assertTrue(leaf.insert((SQLInteger) record4.getValue(0), record4));

      AbstractIndexElement<SQLInteger> indexElement1 = leaf.createInstance();
      AbstractIndexElement<SQLInteger> indexElement2 = leaf.createInstance();
      leaf.split(indexElement1, indexElement2);

      // create new (inner) node
      Node<SQLInteger> root = new Node<SQLInteger>(index);
      AbstractRecord nodeRecord = index.getNodeRecPrototype().clone();
      nodeRecord.setValue(UniqueBPlusTreeBase.KEY_POS, indexElement1.getMaxKey());
      nodeRecord.setValue(
          UniqueBPlusTreeBase.PAGE_POS, new SQLInteger(indexElement1.getPageNumber()));
      root.getIndexPage().insert(nodeRecord);
      nodeRecord.setValue(UniqueBPlusTreeBase.KEY_POS, indexElement2.getMaxKey());
      nodeRecord.setValue(
          UniqueBPlusTreeBase.PAGE_POS, new SQLInteger(indexElement2.getPageNumber()));
      root.getIndexPage().insert(nodeRecord);

      // test lookup

      // insert new record
      Assertions.assertNull(root.lookup((SQLInteger) record1.getValue(0)));
      Assertions.assertTrue(root.insert((SQLInteger) record1.getValue(0), record1));
      Assertions.assertEquals(record1, root.lookup((SQLInteger) record1.getValue(0)));
    }
  }
}
