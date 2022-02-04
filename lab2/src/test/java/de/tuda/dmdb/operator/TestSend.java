package de.tuda.dmdb.operator;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.operator.exercise.Send;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.EnumSQLType;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSend extends TestCase {

  /** Test that Send Operator invokes distribution function and sends data or keeps it local */
  @Test
  public void testSendNoPeerReturnLocalData() {
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
    int port = 8400;
    Map<Integer, String> nodeMap = new HashMap<Integer, String>();
    nodeMap.put(nodeId, "localhost:" + port);
    int attColumn = 0;
    Function<AbstractRecord, List<Integer>> distributionFunction =
        record -> {
          if (attColumn >= record.getValues().length) {
            throw new IllegalArgumentException(
                "Illegal partitioning Column, specified column number to high");
          }

          int value = 0;
          EnumSQLType type = record.getValue(attColumn).getType();
          if (type == EnumSQLType.SqlInteger) {
            value = ((SQLInteger) record.getValue(attColumn)).getValue();
          }
          if (type == EnumSQLType.SqlVarchar) {
            value = ((SQLVarchar) record.getValue(attColumn)).getValue().hashCode();
          }
          List<Integer> result = new ArrayList<Integer>();
          // set to 2, to simulate connection to a peer
          result.add(value % 2);

          return result;
        };

    Send sendOperator = new Send(tsLeft, nodeId, nodeMap, distributionFunction);
    sendOperator.open();

    AbstractRecord next;
    List<AbstractRecord> recordList = new ArrayList<AbstractRecord>();
    while ((next = sendOperator.next()) != null) {
      recordList.add(next);
    }

    Assertions.assertTrue(
        recordList.contains(recordLeft1), "The first record should be in the result");
    Assertions.assertFalse(
        recordList.contains(recordLeft2), "The second record should not be in the result");

    sendOperator.close();
  }
}
