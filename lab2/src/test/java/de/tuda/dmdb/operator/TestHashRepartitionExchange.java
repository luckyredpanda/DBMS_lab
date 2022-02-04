package de.tuda.dmdb.operator;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.operator.exercise.HashRepartitionExchange;
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

public class TestHashRepartitionExchange extends TestCase {

  /**
   * Test that HashRepartition shuffles data to correct nodes
   *
   * @throws InterruptedException
   */
  @Test
  public void testHashRepartitionSimple1() throws InterruptedException {

    AbstractRecord templateRecord = new Record(1);
    templateRecord.setValue(0, new SQLInteger(0));

    HeapTable htable1 = new HeapTable(templateRecord);
    HeapTable htable2 = new HeapTable(templateRecord);

    Queue<AbstractRecord> resultList = new LinkedList<AbstractRecord>();
    Queue<AbstractRecord> resultList2 = new LinkedList<AbstractRecord>();
    Queue<AbstractRecord> expectedResult = new LinkedList<AbstractRecord>();
    Queue<AbstractRecord> expectedResult2 = new LinkedList<AbstractRecord>();

    int numRecords = 200;
    for (int i = 0; i < numRecords; i++) {

      AbstractRecord record = new Record(1);
      record.setValue(0, new SQLInteger(i));

      if (i < numRecords / 2) {
        htable1.insert(record);
      } else {
        htable2.insert(record);
      }

      int hashValue = i % 2;
      if (hashValue == 0) {
        expectedResult.add(record);
      } else {
        expectedResult2.add(record);
      }
    }

    int nodeId = 0;
    int port = 8100;
    Map<Integer, String> nodeMap = new HashMap<Integer, String>();
    nodeMap.put(nodeId, "localhost:" + port);
    nodeMap.put(nodeId + 1, "localhost:" + (port + 1));
    int partitionColumn = 0;

    Runnable task1 =
        () -> {
          TableScan tableScan = new TableScan(htable1);
          HashRepartitionExchange shuffleOperator =
              new HashRepartitionExchange(tableScan, nodeId, nodeMap, port, partitionColumn);
          shuffleOperator.open();
          AbstractRecord next;
          while ((next = shuffleOperator.next()) != null) {
            resultList.offer(next);
          }
          shuffleOperator.close();
        };

    Runnable task2 =
        () -> {
          TableScan tableScan = new TableScan(htable2);
          HashRepartitionExchange shuffleOperator =
              new HashRepartitionExchange(
                  tableScan, nodeId + 1, nodeMap, port + 1, partitionColumn);
          shuffleOperator.open();
          AbstractRecord next;
          while ((next = shuffleOperator.next()) != null) {
            resultList2.offer(next);
          }
          shuffleOperator.close();
        };

    Thread peer1 = new Thread(task1);
    Thread peer2 = new Thread(task2);
    peer1.start();
    peer2.start();
    peer1.join();
    peer2.join();

      System.out.println("这里是1");
      System.out.println(resultList);

      System.out.println("这里是1");
      System.out.println(expectedResult);

      System.out.println("这里是2");
      System.out.println(resultList2);

      System.out.println("这里是2");
      System.out.println(expectedResult2);

    Assertions.assertEquals(numRecords / 2, resultList.size());
    Assertions.assertEquals(numRecords / 2, resultList2.size());
    for (AbstractRecord expectedRec : expectedResult) {
      Assertions.assertTrue(
          resultList.contains(expectedRec), "Result list does not contain expected record");
    }
    for (AbstractRecord expectedRec : expectedResult2) {
      Assertions.assertTrue(
          resultList2.contains(expectedRec), "Result list does not contain expected record");
    }
  }
}
