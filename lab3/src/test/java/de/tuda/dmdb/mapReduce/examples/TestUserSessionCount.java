package de.tuda.dmdb.mapReduce.examples;

import static de.tuda.dmdb.mapReduce.examples.MapReduceTestHelper.*;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.mapReduce.Configuration;
import de.tuda.dmdb.mapReduce.Job;
import de.tuda.dmdb.mapReduce.exercise.UserSessionCount;
import de.tuda.dmdb.storage.AbstractRecord;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestUserSessionCount extends TestCase {

  @Test
  public void testSimple() {
    // init partition
    HeapTable table = new HeapTable(getRecord(0, 0));
    table.insert(getRecord(1, 1));
    table.insert(getRecord(3, 6));
    table.insert(getRecord(3, 100));

    List<AbstractRecord> expectedList = new ArrayList<>();
    expectedList.add(getRecord(1, 1));
    expectedList.add(getRecord(3, 2));

    List<HeapTable> partitions = new ArrayList<>();
    partitions.add(table);

    UserSessionCount userSessionCount = new UserSessionCount();
    Job userSessionCountJob = userSessionCount.createJob(new Configuration());
    userSessionCountJob.setInputs(partitions);
    userSessionCountJob.setOutputs(new ArrayList<>());
    try {
      userSessionCountJob.submit();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Assertions.assertTrue(userSessionCountJob.waitForCompletion());
    Assertions.assertTrue(userSessionCountJob.isComplete());

    List<HeapTable> outputs = userSessionCountJob.getOutputs();

    // compare results
    Assertions.assertEquals(1, outputs.size(), "Expected one result partition.");

    compareResultSet(expectedList, outputs);
  }
}
