package de.tuda.dmdb.buffer;

import de.tuda.dmdb.buffer.exercise.LRUReplacement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLRUReplacement {

  /**
   * Test that fix() throws a runtime exception if unexpectedly called when no free frames are
   * available
   */
  @Test
  public void testFixingTooManyFrames() {
    final int POOL_SIZE = 4;
    LRUReplacement replacer = new LRUReplacement(POOL_SIZE);
    // fill clock to capacity
    for (int i = 0; i < POOL_SIZE; i++) {
      replacer.fix(i);
    }
    // try fixing a new page
    Assertions.assertThrows(
        RuntimeException.class,
        () -> replacer.fix(POOL_SIZE + 1),
        "Fixing more pages than available frames should throw a RuntimeException!");
  }

  /**
   * Test that evict() evicts the first fixed page, after filling the clock initially The evicted
   * page should be recorded in the evictedPages vector
   */
  @Test
  public void testEviction() {
    final int POOL_SIZE = 4;
    LRUReplacement replacer = new LRUReplacement(POOL_SIZE);
    // fill clock to capacity
    for (int i = 0; i < POOL_SIZE; i++) {
      replacer.fix(i);
    }
    Integer victim = replacer.evict();
    Assertions.assertEquals(Integer.valueOf(0), victim, "Evicted page should be the first page");
    Assertions.assertEquals(
        victim, replacer.getEvictedPages().get(0), "Evicted page has been recorded");
  }
}
