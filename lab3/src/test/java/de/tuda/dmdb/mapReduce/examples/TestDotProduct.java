package de.tuda.dmdb.mapReduce.examples;

import static de.tuda.dmdb.mapReduce.examples.MapReduceTestHelper.getRecord;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.mapReduce.Configuration;
import de.tuda.dmdb.mapReduce.Job;
import de.tuda.dmdb.mapReduce.exercise.DotProduct;
import de.tuda.dmdb.storage.AbstractRecord;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDotProduct extends TestCase {
  @Test
  public void testSimple() {
    // init partition
    HeapTable table = new HeapTable(getRecord(0, 0));
    table.insert(getRecord(5, 1));
    table.insert(getRecord(3, 6));
    table.insert(getRecord(10, 10));

    List<AbstractRecord> expectedList = new ArrayList<>();
    expectedList.add(getRecord(1, 123));

    List<HeapTable> partitions = new ArrayList<>();
    partitions.add(table);

    DotProduct dotProduct = new DotProduct();
    Job dotProductJob = dotProduct.createJob(new Configuration());
    dotProductJob.setInputs(partitions);
    dotProductJob.setOutputs(new ArrayList<>());
    try {
      dotProductJob.submit();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Assertions.assertTrue(dotProductJob.waitForCompletion());
    Assertions.assertTrue(dotProductJob.isComplete());

    List<HeapTable> outputs = dotProductJob.getOutputs();

    // compare results
    Assertions.assertEquals(1, outputs.size(), "Expected one result partition.");

    MapReduceTestHelper.compareResultSet(expectedList, outputs);
  }
}
