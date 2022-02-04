package de.tuda.dmdb.mapReduce.task;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.mapReduce.task.exercise.ShuffleSortTask;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Vector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestShuffleSortTask extends TestCase {

  @Test
  public void testShuffleSortSimple() {

    AbstractRecord templateRecord = new Record(1);
    templateRecord.setValue(0, new SQLInteger(0));
    templateRecord.setValue(0, new SQLInteger(0));

    int partitionColumn = 0;
    int startPort = 8000;
    int numNodes = 4;
    int numReducers = 2;
    int numRecords = 200;
    Map<Integer, String> nodeMap = new HashMap<Integer, String>();

    Vector<Queue<AbstractRecord>> partitionLists = new Vector<Queue<AbstractRecord>>();
    Vector<Queue<AbstractRecord>> resultLists = new Vector<Queue<AbstractRecord>>();
    Vector<Queue<AbstractRecord>> expectedResults = new Vector<Queue<AbstractRecord>>();

    Vector<HeapTable> hTables = new Vector<HeapTable>();

    for (int i = 0; i < numNodes; i++) {
      hTables.add(new HeapTable(templateRecord));
      nodeMap.put(i, "localhost:" + (startPort + i));
      partitionLists.add(new LinkedList<AbstractRecord>());
      resultLists.add(new LinkedList<AbstractRecord>());
      expectedResults.add(new LinkedList<AbstractRecord>());
    }

    for (int i = 0; i < numRecords; i++) {

      AbstractRecord recrod = new Record(1);
      recrod.setValue(0, new SQLInteger(i));
      recrod.setValue(0, new SQLInteger(i));

      for (int j = numNodes - 1; j >= 0; j--) {
        int bound = (numRecords / numNodes) * j;
        if (i >= bound) {
          partitionLists.get(j).add(recrod);
          hTables.get(j).insert(recrod);
          break;
        }
        continue;
      }
      int hashValue = i % numNodes;
      expectedResults.get(hashValue).add(recrod);
    }

    List<HeapTable> outputs = new ArrayList<HeapTable>();
    for (int i = 0; i < numNodes; i++) {
      outputs.add(new HeapTable(templateRecord));
    }

    Vector<Thread> peerThreads = new Vector<Thread>();

    for (int i = 0; i < numNodes; i++) {
      final int j = i;
      int nodeId = j;
      HeapTable input = hTables.get(j);
      HeapTable output = outputs.get(j);
      peerThreads.add(
          new ShuffleSortTask(input, output, nodeId, nodeMap, partitionColumn, numReducers));
    }
    for (Thread thread : peerThreads) {
      thread.start();
    }
    for (Thread thread : peerThreads) {
      try {
        thread.join();
      } catch (Exception e) {
        e.printStackTrace();
        Assertions.assertTrue(false);
      }
    }
  }
}
