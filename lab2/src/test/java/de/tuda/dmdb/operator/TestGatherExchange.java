package de.tuda.dmdb.operator;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.operator.exercise.GatherExchange;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGatherExchange extends TestCase {

  /**
   * Test that GatherExchange sends data to coordinator
   *
   * @throws InterruptedException
   */
  @Test
  public void testSimple1() throws InterruptedException {

    AbstractRecord templateRecord = new Record(1);
    templateRecord.setValue(0, new SQLInteger(0));

    HeapTable htable1 = new HeapTable(templateRecord);
    HeapTable htable2 = new HeapTable(templateRecord);

    Queue<AbstractRecord> resultList = new LinkedList<AbstractRecord>();
    Queue<AbstractRecord> resultList2 = new LinkedList<AbstractRecord>();
    Queue<AbstractRecord> expectedResult = new LinkedList<AbstractRecord>();
    Queue<AbstractRecord> expectedResult2 = new LinkedList<AbstractRecord>();

    int nodeId = 0;
    int port = 8000;
    Map<Integer, String> nodeMap = new HashMap<Integer, String>();
    nodeMap.put(nodeId, "localhost:" + port);
    nodeMap.put(nodeId + 1, "localhost:" + (port + 1));

    int numRecords = 10;
    for (int i = 0; i < numRecords; i++) {

      AbstractRecord record = new Record(1);
      record.setValue(0, new SQLInteger(i));

      // distribute records round roubin
      if ((i % 2) == 0) {
        htable1.insert(record);

      } else {
        htable2.insert(record);
      }

      // all records are send to node 0, no records are sent to node1
      expectedResult.add(record);
    }

    int coordinatorId = 0;

    Runnable task1 =
        () -> {
          TableScan tableScan = new TableScan(htable1);
          GatherExchange exchange =
              new GatherExchange(tableScan, nodeId, nodeMap, port, coordinatorId);
          exchange.open();
          AbstractRecord next;
          while ((next = exchange.next()) != null) {
            resultList.offer(next);
          }
          exchange.close();
        };

    Runnable task2 =
        () -> {
          TableScan tableScan = new TableScan(htable2);
          GatherExchange exchange =
              new GatherExchange(tableScan, nodeId + 1, nodeMap, port + 1, coordinatorId);
          exchange.open();
          AbstractRecord next;
          while ((next = exchange.next()) != null) {
            resultList2.offer(next);
          }
          exchange.close();
        };

    Thread peer1 = new Thread(task1);
    Thread peer2 = new Thread(task2);
    peer1.start();
    peer2.start();
    peer1.join();
    peer2.join();



    Assertions.assertEquals(
        expectedResult.size(),
        resultList.size(),
        "All records to should have been sent to node 0, number of records: ");
    Assertions.assertEquals(
        expectedResult2.size(),
        resultList2.size(),
        "No records to should have been sent to node 1, number of records: ");
    // order of records in expectedResult and resultList could be different, hence
    // use a for loop to compare
    for (AbstractRecord expectedRec : expectedResult) {
      Assertions.assertTrue(
          resultList.contains(expectedRec), "Expected record should be contained in resultList");
    }
  }
}
