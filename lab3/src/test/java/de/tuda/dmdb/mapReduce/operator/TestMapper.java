package de.tuda.dmdb.mapReduce.operator;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.mapReduce.operator.exercise.Mapper;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMapper extends TestCase {

  @Test
  public void testMapperInit() {

    AbstractRecord record = MapReduceOperator.keyValueRecordPrototype.clone();
    record.setValue(MapReduceOperator.KEY_COLUMN, new SQLInteger(0));
    record.setValue(MapReduceOperator.VALUE_COLUMN, new SQLVarchar("the quick brown fox", 100));

    HeapTable table1 = new HeapTable(record);
    table1.insert(record);

    TableScan ts = new TableScan(table1);
    Mapper<SQLInteger, SQLVarchar, SQLVarchar, SQLInteger> mapper = new Mapper<>();
    mapper.setChild(ts);

    Assertions.assertThrows(
        NullPointerException.class,
        mapper::next,
        "NullPointerException should have been thrown, since open() has not been called yet.");
    mapper.open();

    AbstractRecord next = mapper.next();
    Assertions.assertEquals(record, next); // identity is default behavior
    mapper.close();
  }

  @Test
  public void testMapperIdentity() {

    List<AbstractRecord> expectedResult = new ArrayList<AbstractRecord>();
    List<AbstractRecord> actualResult = new ArrayList<AbstractRecord>();

    AbstractRecord record = MapReduceOperator.keyValueRecordPrototype.clone();
    record.setValue(MapReduceOperator.KEY_COLUMN, new SQLInteger(0));
    record.setValue(MapReduceOperator.VALUE_COLUMN, new SQLVarchar("the quick brown fox", 100));

    AbstractRecord expectedRecord = record.clone();
    expectedResult.add(expectedRecord);

    HeapTable table1 = new HeapTable(record);
    table1.insert(record);

    TableScan ts = new TableScan(table1);
    Mapper<SQLInteger, SQLVarchar, SQLVarchar, SQLInteger> mapper = new Mapper<>();
    mapper.setChild(ts);
    mapper.open();
    AbstractRecord next;
    while ((next = mapper.next()) != null) {
      actualResult.add(next);
    }
    mapper.close();

    Assertions.assertEquals(expectedResult.size(), actualResult.size());

    for (int i = 0; i < actualResult.size(); i++) {
      AbstractRecord expectedRec = expectedResult.get(i);
      AbstractRecord value2 = actualResult.get(i);
      Assertions.assertEquals(expectedRec, value2);
    }
  }
}
