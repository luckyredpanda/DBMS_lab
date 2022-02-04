package de.tuda.dmdb.buffer;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.buffer.exercise.BufferManager;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import java.util.Random;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestBufferManager extends TestCase {

  /** Test that the number of free buffer frames is reduced by creating new pages */
  @Test
  public void testNumberOfFreeBuffersIsCorrectlyMaintained() {
    final Integer POOL_SIZE = 4;
    AbstractRecord record = new Record(1);
    record.setValue(0, new SQLInteger(123456789));
    final int SLOT_SIZE = record.getFixedLength();

    BufferManager.clearInstance();
    BufferManager.POOL_SIZE = POOL_SIZE;
    BufferManagerBase bufferManager = BufferManager.getInstance();
    Assertions.assertEquals(
        POOL_SIZE, bufferManager.getBufferPoolSize(), "Pool size should have changed");
    Assertions.assertEquals(
        POOL_SIZE,
        bufferManager.getFreeFrames(),
        "Pool should be empty, number of free frames should be");

    // create new pages
    Integer previousFreeBuffers = bufferManager.getFreeFrames();
    for (int i = 0; i < POOL_SIZE; i++) {
      bufferManager.createDefaultPage(SLOT_SIZE);
      // number of buffer frames should have been decremented
      Integer expected = previousFreeBuffers - 1;
      Assertions.assertEquals(
          expected, bufferManager.getFreeFrames(), "Number of free buffers is being decremented");
      previousFreeBuffers = bufferManager.getFreeFrames();
    }
  }

  /** Test that unpinning a page will make it a victim for the next eviction */
  @Test
  public void testUnpin() {
    final Integer POOL_SIZE = 4;
    AbstractRecord record = new Record(1);
    record.setValue(0, new SQLInteger(123456789));
    final int SLOT_SIZE = record.getFixedLength();

    BufferManager.clearInstance();
    BufferManager.POOL_SIZE = POOL_SIZE;
    BufferManagerBase bufferManager = BufferManager.getInstance();
    Assertions.assertEquals(
        POOL_SIZE, bufferManager.getBufferPoolSize(), "Pool size should have changed");
    Assertions.assertEquals(
        POOL_SIZE,
        bufferManager.getFreeFrames(),
        "Pool should be empty, number of free frames should be");

    // create new pages
    for (int i = 0; i < POOL_SIZE; i++) {
      bufferManager.createDefaultPage(SLOT_SIZE);
    }
    Assertions.assertEquals(
        Integer.valueOf(0),
        bufferManager.getFreeFrames(),
        "Buffer should be full, number of free frames should be");
    // unfix a random page
    Random rand = new Random();
    Integer randomPage = rand.nextInt(POOL_SIZE);
    bufferManager.unpin(randomPage);
    // pinning a new page should evict the previously unpinned page first
    bufferManager.createDefaultPage(SLOT_SIZE);
    Integer victim = bufferManager.getReplacer().getEvictedPages().get(0);
    Assertions.assertEquals(randomPage, victim, "Victim matches unpinned page");
  }
}
