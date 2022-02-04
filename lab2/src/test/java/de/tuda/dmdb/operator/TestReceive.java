package de.tuda.dmdb.operator;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.operator.exercise.Receive;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for the Receive operator
 *
 * @author melhindi
 */
public class TestReceive extends TestCase {

  /** Test that both local and remote elements (in the localCache) are being processed */
  @Test
  public void testReceiveProcessLocalAndRemoteRecords() {
    AbstractRecord recordLeft1 = new Record(3);
    recordLeft1.setValue(0, new SQLInteger(0));
    recordLeft1.setValue(1, new SQLInteger(0));
    recordLeft1.setValue(2, new SQLInteger(0));

    AbstractRecord recordLeft2 = new Record(3);
    recordLeft2.setValue(0, new SQLInteger(1));
    recordLeft2.setValue(1, new SQLInteger(1));
    recordLeft2.setValue(2, new SQLInteger(1));

    HeapTable htLeft = new HeapTable(recordLeft1);

    htLeft.insert(recordLeft1);
    htLeft.insert(recordLeft2);

    TableScan tsLeft = new TableScan(htLeft);

    int nodeId = 0;
    int port = 8200;
    Map<Integer, String> nodeMap = new HashMap<Integer, String>();
    nodeMap.put(nodeId, "localhost:" + port);
    // create and fill a mock localCache for the receive operator
    ConcurrentLinkedQueue<AbstractRecord> localCache = new ConcurrentLinkedQueue<AbstractRecord>();
    AbstractRecord remoteRecord = new Record(3);
    remoteRecord.setValue(0, new SQLInteger(1));
    remoteRecord.setValue(1, new SQLInteger(1));
    remoteRecord.setValue(2, new SQLInteger(1));
    localCache.add(remoteRecord);

    Receive receiveOperator = new Receive(tsLeft, nodeMap.size(), port, nodeId);

    receiveOperator.open();
    receiveOperator.setLocalCache(localCache);

    AbstractRecord next;
    Queue<AbstractRecord> resultList = new LinkedList<AbstractRecord>();
    Queue<AbstractRecord> expectedResult = new LinkedList<AbstractRecord>();
    // expectedResult should contain local and remote elements
    expectedResult.add(recordLeft1);
    expectedResult.add(recordLeft2);
    expectedResult.add(remoteRecord);
    while ((next = receiveOperator.next()) != null) {
      resultList.add(next);
    }
    for (AbstractRecord expectedRec : expectedResult) {
      Assertions.assertTrue(
          resultList.contains(expectedRec), "Result list does not contain expected record");
    }

    receiveOperator.close();
  }
}
