package de.tuda.dmdb.mapReduce.examples;

import static de.tuda.dmdb.mapReduce.examples.MapReduceTestHelper.*;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.mapReduce.Configuration;
import de.tuda.dmdb.mapReduce.Job;
import de.tuda.dmdb.mapReduce.exercise.WordCounter;
import de.tuda.dmdb.storage.AbstractRecord;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestWordCounter extends TestCase {

  @Test
  public void testSimple() {
    // init partition
    HeapTable table = new HeapTable(getRecord(0, "abc"));

    table.insert(getRecord(0, "this is a sample text"));
    table.insert(getRecord(1, "having fun with text"));

    List<AbstractRecord> expectedList = new ArrayList<>();
    expectedList.add(getRecord("this", 1));
    expectedList.add(getRecord("is", 1));
    expectedList.add(getRecord("a", 1));
    expectedList.add(getRecord("sample", 1));
    expectedList.add(getRecord("text", 2));
    expectedList.add(getRecord("having", 1));
    expectedList.add(getRecord("fun", 1));
    expectedList.add(getRecord("with", 1));

    List<HeapTable> partitions = new ArrayList<>();
    partitions.add(table);

    WordCounter wordCounter = new WordCounter();
    Job wordCounterJob = wordCounter.createJob(new Configuration());
    wordCounterJob.setInputs(partitions);
    wordCounterJob.setOutputs(new ArrayList<>());
    try {
      wordCounterJob.submit();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Assertions.assertTrue(wordCounterJob.waitForCompletion());
    Assertions.assertTrue(wordCounterJob.isComplete());

    List<HeapTable> outputs = wordCounterJob.getOutputs();
    System.out.println("expected"+expectedList);
    System.out.println("result"+outputs);

    // compare results
    Assertions.assertEquals(1, outputs.size(), "Expected one result partition.");

    compareResultSet(expectedList, outputs);
  }
}
