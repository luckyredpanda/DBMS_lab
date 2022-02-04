package de.tuda.dmdb.execution;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.execution.exercise.SymmetricRepartitioningJoinPlan;
import de.tuda.dmdb.operator.Operator;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSymmetricRepartitioningJoinPlan extends TestCase {

  /**
   * Test that correct local results are generated
   *
   * @throws InterruptedException
   */
  @Test
  public void testSymmetricRepartitioningJoin() throws InterruptedException {
    AbstractRecord templateRecord = new Record(2);
    templateRecord.setValue(0, new SQLInteger(0));
    templateRecord.setValue(1, new SQLVarchar(1));

    Queue<AbstractRecord> resultList = new LinkedList<AbstractRecord>();
    Queue<AbstractRecord> resultList2 = new LinkedList<AbstractRecord>();
    Queue<AbstractRecord> expectedResult = new LinkedList<AbstractRecord>();
    Queue<AbstractRecord> expectedResult2 = new LinkedList<AbstractRecord>();

    // create two tables with two partitions each
    HeapTable customers1 = new HeapTable(templateRecord);
    HeapTable customers2 = new HeapTable(templateRecord);
    HeapTable orders1 = new HeapTable(templateRecord);
    HeapTable orders2 = new HeapTable(templateRecord);

    // fill partitions
    AbstractRecord customerRecord1 = templateRecord.clone();
    customerRecord1.setValue(0, new SQLInteger(0));
    customerRecord1.setValue(1, new SQLVarchar("C", 1));
    customers2.insert(customerRecord1);

    AbstractRecord customerRecord2 = templateRecord.clone();
    customerRecord2.setValue(0, new SQLInteger(1));
    customerRecord2.setValue(1, new SQLVarchar("C", 1));
    customers1.insert(customerRecord2);

    AbstractRecord ordersRecord1 = templateRecord.clone();
    ordersRecord1.setValue(0, new SQLInteger(0));
    ordersRecord1.setValue(1, new SQLVarchar("O", 1));
    orders2.insert(ordersRecord1);

    AbstractRecord ordersRecord2 = templateRecord.clone();
    ordersRecord2.setValue(0, new SQLInteger(1));
    ordersRecord2.setValue(1, new SQLVarchar("O", 1));
    orders1.insert(ordersRecord2);

    expectedResult.add(customerRecord1.append(ordersRecord1));
    expectedResult2.add(customerRecord2.append(ordersRecord2));

    int nodeId = 0;
    int listenerPortLeft = 6400;
    int listenerPortRight = 6410;
    Map<Integer, String> nodeMapLeft = new HashMap<Integer, String>();
    nodeMapLeft.put(nodeId, "localhost:" + listenerPortLeft);
    nodeMapLeft.put(nodeId + 1, "localhost:" + (listenerPortLeft + 1));
    Map<Integer, String> nodeMapRight = new HashMap<Integer, String>();
    nodeMapRight.put(nodeId, "localhost:" + listenerPortRight);
    nodeMapRight.put(nodeId + 1, "localhost:" + (listenerPortRight + 1));
    int leftJoinAtt = 0;
    int rightJoinAtt = 0;
    List<Operator> rootOperators = Collections.synchronizedList(new ArrayList<Operator>());

    Runnable task1 =
        () -> {
          TableScan tableScanCustomers = new TableScan(customers1);
          TableScan tableScanOrders = new TableScan(orders2);
          SymmetricRepartitioningJoinPlan localJoin1 =
              new SymmetricRepartitioningJoinPlan(
                  tableScanCustomers,
                  tableScanOrders,
                  leftJoinAtt,
                  rightJoinAtt,
                  nodeId,
                  listenerPortLeft,
                  nodeMapLeft,
                  listenerPortRight,
                  nodeMapRight);
          Operator root = localJoin1.compilePlan();
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
          TableScan tableScanOrders = new TableScan(orders1);
          SymmetricRepartitioningJoinPlan localJoin1 =
              new SymmetricRepartitioningJoinPlan(
                  tableScanCustomers,
                  tableScanOrders,
                  leftJoinAtt,
                  rightJoinAtt,
                  nodeId + 1,
                  listenerPortLeft + 1,
                  nodeMapLeft,
                  listenerPortRight + 1,
                  nodeMapRight);
          Operator root = localJoin1.compilePlan();
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
    Assertions.assertEquals(1, resultList2.size(), "Result list should contain as many records: ");
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
