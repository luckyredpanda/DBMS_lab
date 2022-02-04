package de.tuda.dmdb.execution;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.execution.exercise.DistributedCountPlan;
import de.tuda.dmdb.operator.Operator;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Vector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDistributedCountPlan extends TestCase {

  /**
   * Test that correct local results are generated
   *
   * @throws InterruptedException
   */
  @Test
  public void testSimple() throws InterruptedException {
    AbstractRecord templateRecord = new Record(1);
    templateRecord.setValue(0, new SQLInteger(0));

    Queue<AbstractRecord> resultList = new LinkedList<AbstractRecord>();
    Queue<AbstractRecord> resultList2 = new LinkedList<AbstractRecord>();
    Queue<AbstractRecord> expectedResult = new LinkedList<AbstractRecord>();
    Queue<AbstractRecord> expectedResult2 = new LinkedList<AbstractRecord>();

    // create two tables with two partitions each
    HeapTable customers1 = new HeapTable(templateRecord);
    HeapTable customers2 = new HeapTable(templateRecord);

    // fill partitions
    AbstractRecord customerRecord1 = templateRecord.clone();
    customerRecord1.setValue(0, new SQLInteger(0));
    customers1.insert(customerRecord1);

    AbstractRecord customerRecord2 = templateRecord.clone();
    customerRecord2.setValue(0, new SQLInteger(1));
    customers2.insert(customerRecord2);

    AbstractRecord expectedResultRecord = new Record(1);
    expectedResultRecord.setValue(0, new SQLInteger(2));
    expectedResult.add(expectedResultRecord);

    int nodeId = 0;
    int listenerPort = 6100;
    Map<Integer, String> nodeMap = new HashMap<Integer, String>();
    nodeMap.put(nodeId, "localhost:" + listenerPort);
    nodeMap.put(nodeId + 1, "localhost:" + (listenerPort + 1));
    int coordinatorId = 0;
    Vector<Integer> groupByAttributes = null;
    Vector<Integer> aggregateAttributes = new Vector<Integer>();
    aggregateAttributes.add(0);
    List<Operator> rootOperators = Collections.synchronizedList(new ArrayList<Operator>());

    Runnable task1 =
        () -> {
          TableScan tableScanCustomers = new TableScan(customers1);
          DistributedCountPlan localPlan =
              new DistributedCountPlan(
                  tableScanCustomers,
                  nodeId,
                  listenerPort,
                  nodeMap,
                  coordinatorId,
                  groupByAttributes,
                  aggregateAttributes);
          Operator root = localPlan.compilePlan();
          rootOperators.add(root);
          root.open();
          AbstractRecord next;
          while ((next = root.next()) != null) {
            resultList.offer(next);
          }
        };
    Runnable task2 =
        () -> {
          TableScan tableScanCustomers = new TableScan(customers2);
          DistributedCountPlan localPlan =
              new DistributedCountPlan(
                  tableScanCustomers,
                  nodeId + 1,
                  listenerPort + 1,
                  nodeMap,
                  coordinatorId,
                  groupByAttributes,
                  aggregateAttributes);
          Operator root = localPlan.compilePlan();
          rootOperators.add(root);
          root.open();
          AbstractRecord next;
          while ((next = root.next()) != null) {
            resultList2.offer(next);
          }
        };

    Thread peer1 = new Thread(task1);
    Thread peer2 = new Thread(task2);
    peer1.start();
    peer2.start();
    peer1.join();
    peer2.join();
    // both nodes finished, tell both nodes to close
    for (Operator operator : rootOperators) {
      operator.close();
    }
      System.out.println("1 r"+resultList);
      System.out.println("1 e"+expectedResult);
      System.out.println("2 r"+resultList2);
      System.out.println("2 e"+expectedResult2);

    Assertions.assertEquals(1, resultList.size(), "Result list should contain as many records: ");
    Assertions.assertEquals(0, resultList2.size(), "Result list should contain as many records: ");
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
