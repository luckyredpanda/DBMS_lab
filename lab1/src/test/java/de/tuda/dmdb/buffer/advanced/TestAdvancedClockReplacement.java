package de.tuda.dmdb.buffer.advanced;

import org.junit.jupiter.api.Test;

public class TestAdvancedClockReplacement {

  /** Test that evict() frees a frame for a subsequent fix() operations */
  @Test
  public void testFixAfterEviction() {
    // fill clock to capacity

    // try fixing a new page - expect Exception

    // fixing a new page should be possible after eviction
  }

  /**
   * Test that calling evict() twice (on the same state) does not move the clock hand, since there
   * are free buffer frames
   */
  @Test
  public void testEvictionCalledTwiseShouldNotMoveHand() {
    // fill clock to capacity
    // call evict() twice, check evictedPages size
  }

  /** Test that calling fix() twice (on the same page) does not change the refBit */
  @Test
  public void testFixCalledTwiseShouldNotChangeRefBit() {
    // fill clock to capacity
    // call fix() twice
    // now determine a victim
    // Evicted page should be the first page despite fix called twice
  }

  /**
   * Test that calling unfix() keeps pages in buffer and if called twice (on the same page) it does
   * not change the refBit
   */
  @Test
  public void testUnfixCalledTwiseShouldNotChangeRefBit() {
    // fill clock to capacity, and immediately unfix
    // call unfix() twice on one of the pages
    // now determine a victim
    // Evicted page should be the first page despite unfix called twice on one page
  }

  /** Test that unfixing a page will make it a victim for the next eviction */
  @Test
  public void testUnfix() {
    // fill clock to capacity
    // unfix a random page
    // calling evict should evict the previously unfixed page first
  }

  /** Test unfixing an unknown or previously evicted page is handled gracefully */
  @Test
  public void testUnfixingAnUnknownPage() {

    // unfix an unknown page
    // we expect that simply nothing will happen
    // some students might throw an exception which is ok

    // fill clock to capacity

    // unfix an unknown page
    // we expect that simply nothing will happen
    // some students might throw an exception which is ok

    // unfixing a known page should still work

    // calling evict should evict the previously unfixed page first

    // unfix the previouly evicted page, i.e., it is now unknown
    // we expect that simply nothing will happen
    // some students might throw an exception which is ok
  }
}
