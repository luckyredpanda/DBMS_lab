package de.tuda.dmdb.mapReduce.examples;

import static de.tuda.dmdb.mapReduce.examples.MapReduceTestHelper.*;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.mapReduce.Configuration;
import de.tuda.dmdb.mapReduce.Job;
import de.tuda.dmdb.mapReduce.exercise.UserAverageSessionLength;
import de.tuda.dmdb.storage.AbstractRecord;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestUserAverageSessionLength extends TestCase {
  @Test
  public void testSimple() {
    // init partition
    HeapTable table = new HeapTable(getRecord(0, 0.0));
    table.insert(getRecord(5, 1.0));
    table.insert(getRecord(3, 6.0));
    table.insert(getRecord(3, 10.0));

    List<AbstractRecord> expectedList = new ArrayList<>();
    expectedList.add(getRecord(5, 1.0));
    expectedList.add(getRecord(3, 8.0));

    List<HeapTable> partitions = new ArrayList<>();
    partitions.add(table);

    UserAverageSessionLength userAverageSessionLength = new UserAverageSessionLength();
    Job userAverageSessionLengthJob = userAverageSessionLength.createJob(new Configuration());
    userAverageSessionLengthJob.setInputs(partitions);
    userAverageSessionLengthJob.setOutputs(new ArrayList<>());
    try {
      userAverageSessionLengthJob.submit();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Assertions.assertTrue(userAverageSessionLengthJob.waitForCompletion());
    Assertions.assertTrue(userAverageSessionLengthJob.isComplete());

    List<HeapTable> outputs = userAverageSessionLengthJob.getOutputs();

    // compare results
    Assertions.assertEquals(1, outputs.size(), "Expected one result partition.");

    compareResultSet(expectedList, outputs);
  }
}
