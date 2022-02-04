package de.tuda.dmdb.mapReduce.examples.advanced;

import static de.tuda.dmdb.mapReduce.examples.MapReduceTestHelper.*;

import de.tuda.dmdb.TestCase;
import java.util.*;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.Disabled;
@Disabled("Disabled until you implement advanced tests") 
public class TestAdvancedWordCounter extends TestCase {

  /** Test processing with empty table returns empty result */
  @Test
  public void testSimpleWordCountEmpty() {}

  /** Runs WordCounter with multiple partitions and multiple records each having random text. */
  @Test
  public void testMultiplePartitionsWithRecords() {}
}
