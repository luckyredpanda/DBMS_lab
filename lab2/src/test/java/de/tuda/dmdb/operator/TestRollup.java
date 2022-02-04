package de.tuda.dmdb.operator;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.operator.exercise.Rollup;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.SQLNull;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for the Rollup operator
 *
 * @author melhindi
 */
public class TestRollup extends TestCase {

  private AbstractRecord getRecordFromVector(Integer... values) {
    AbstractRecord rec = new Record(values.length);
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) rec.setValue(i, new SQLNull());
      else rec.setValue(i, new SQLInteger(values[i]));
    }
    return rec;
  }

  /** Test simple group by on one column and count */
  @Test
  public void testSimple() {
    AbstractRecord r1 = getRecordFromVector(1, 5);
    HeapTable ht = new HeapTable(r1);
    ht.insert(r1);
    ht.insert(getRecordFromVector(1, 3));
    ht.insert(getRecordFromVector(2, 1));
    ht.insert(getRecordFromVector(2, 4));
    ht.insert(getRecordFromVector(3, 2));

    TableScan ts = new TableScan(ht);

    List<Integer> groupByAttributes = new ArrayList<>();
    groupByAttributes.add(0);
    List<Integer> aggregateAttributes = new ArrayList<>();
    aggregateAttributes.add(1);
    List<BiFunction<Integer, Integer, Integer>> aggregateFunctions = new ArrayList<>();
    aggregateFunctions.add(
        (old, update) -> {
          if (old == null || old == 0) {
            return update;
          }
          return old < update ? old : update;
        });

    List<AbstractRecord> expectedList = new ArrayList<>();
    expectedList.add(getRecordFromVector(null, 1));
    expectedList.add(getRecordFromVector(1, 3));
    expectedList.add(getRecordFromVector(2, 1));
    expectedList.add(getRecordFromVector(3, 2));

    Rollup operator =
        new Rollup(
            ts, groupByAttributes, aggregateAttributes, aggregateFunctions, aggregateFunctions);

    operator.open();
    List<AbstractRecord> resultList = new ArrayList<>();
    AbstractRecord resultRec;
    while ((resultRec = operator.next()) != null) {
      resultList.add(resultRec);
    }
    operator.close();

    Assertions.assertEquals(
        expectedList.size(), resultList.size(), "Expected equal amount of records.");
    for (AbstractRecord abstractRecord : resultList) {
      Assertions.assertTrue(
          expectedList.remove(abstractRecord),
          abstractRecord + " was not found in the expected records");
    }
    Assertions.assertTrue(
        expectedList.isEmpty(), "We expected more entries to be in the result list");
  }

  /** Test simple count query without grouping */
  @Test
  public void testNoGroupBy() {
    AbstractRecord r1 = new Record(1);
    r1.setValue(0, new SQLInteger(2));
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
            return update;
          }
          return old < update ? old : update;
        });
    Rollup operator =
        new Rollup(
            ts, groupByAttributes, aggregateAttributes, aggregateFunctions, aggregateFunctions);
    operator.open();
    AbstractRecord expectedResult = new Record(1);
    expectedResult.setValue(0, new SQLInteger(1));
    Assertions.assertEquals(
        expectedResult,
        operator.next(),
        "Returned record should have only one column (for the correct aggregate Value)");
    Assertions.assertNull(
        operator.next(), "Returned record should equal null since there is only one group");
    operator.close();
  }
}
