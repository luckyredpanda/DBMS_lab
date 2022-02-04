package de.tuda.dmdb.operator;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.operator.exercise.GroupByAggregate;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for the GroupByAggregate operator
 *
 * @author melhindi
 */
public class TestGroupByAggregate extends TestCase {

  /** Test simple group by on one column and count */
  @Test
  public void testSimple() {
    AbstractRecord r1 = new Record(1);
    r1.setValue(0, new SQLInteger(1));
    AbstractRecord r2 = new Record(1);
    r2.setValue(0, new SQLInteger(1));

    HeapTable ht = new HeapTable(r1);
    ht.insert(r1);
    ht.insert(r2);

    TableScan ts = new TableScan(ht);

    List<Integer> groupByAttributes = new ArrayList<>();
    groupByAttributes.add(0);
    List<Integer> aggregateAttributes = new ArrayList<>();
    aggregateAttributes.add(0);
    List<BiFunction<Integer, Integer, Integer>> aggregateFunctions = new ArrayList<>();
    aggregateFunctions.add(
        (old, update) -> {
          if (old == null || old == 0) {
            return 1;
          }
          return old + 1;
        });
    GroupByAggregate operator =
        new GroupByAggregate(ts, groupByAttributes, aggregateAttributes, aggregateFunctions);
    operator.open();
    AbstractRecord expectedResult = new Record(2);
    expectedResult.setValue(0, new SQLInteger(1));
    expectedResult.setValue(1, new SQLInteger(2));

    System.out.println("这里是1");
    //System.out.println(operator.next());
    System.out.println("expectedResult"+expectedResult);

    Assertions.assertEquals(
        expectedResult,
        operator.next(),
        "Returned record should have two columns and the correct aggregate value");
    Assertions.assertNull(
        operator.next(), "Returned record should equal null since there is only one group");
    operator.close();
  }

  /** Test simple count query without grouping */
  @Test
  public void testNoGroupBy() {
    AbstractRecord r1 = new Record(1);
    r1.setValue(0, new SQLInteger(0));
    AbstractRecord r2 = new Record(1);
    r2.setValue(0, new SQLInteger(1));

    HeapTable ht = new HeapTable(r1);
    ht.insert(r1);
    ht.insert(r2);

    TableScan ts = new TableScan(ht);

    List<Integer> groupByAttributes = null;
    List<Integer> aggregateAttributes = new ArrayList<>();
    aggregateAttributes.add(0);
    List<BiFunction<Integer, Integer, Integer>> aggregateFunctions = new ArrayList<>();
    aggregateFunctions.add(
        (old, update) -> {
          if (old == null || old == 0) {
            return 1;
          }
          return old + 1;
        });
    GroupByAggregate operator =
        new GroupByAggregate(ts, groupByAttributes, aggregateAttributes, aggregateFunctions);
    operator.open();
    AbstractRecord expectedResult = new Record(1);
    expectedResult.setValue(0, new SQLInteger(2));
    Assertions.assertEquals(
        expectedResult,
        operator.next(),
        "Returned record should have only one column (for the correct aggregate Value)");
    Assertions.assertNull(
        operator.next(), "Returned record should equal null since there is only one group");
    operator.close();
  }
}
