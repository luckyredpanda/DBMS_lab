package de.tuda.dmdb.buffer.advanced;

import de.tuda.dmdb.TestCase;
import org.junit.jupiter.api.Test;

public class TestAdvancedBufferManager extends TestCase {

  /** Test that pinning saves the page from being evicted */
  @Test
  public void testPin() {
    // prepare

    // create new pages

    // pin a new page

    // under most replacement policies this should have evicted the first pinned page

    // under most replacement policies the next victim should be the second pinned page
    // let's pin it and prevent it from being evicted

    // pin a new page an verify that the second pinned paged was not evicted
  }

  /** Test that pinning a page twice does not change the refBit */
  @Test
  public void testClockReplacementRefBitSetToOne() {
    // prepare

    // create new pages

    // pin an existing page, e.g. 0
    // since it was recently pinned, pinning it again should not change the refBit (i.e., do not
    // increment the refBit further)

    // under the clock replacement strategy, still page0 should be evicted, since all refBits were 1
    // initially

    // under most replacement policies the next victim should be the second pinned page
    // let's pin it and prevent it from being evicted

    // pin a new page an verify that the second pinned paged was not evicted
  }

  /** Test that immediate unpinning does not remove a page */
  @Test
  public void testImmediateUnpin() {
    // prepare

    // create new pages

    // pin a new page

    // under most replacement policies this should have evicted the first pinned page

    // under most replacement policies the next victim should be the second pinned page
  }

  /** Test sequence of evicted pages matches example in the lecture */
  @Test
  public void testClockReplacementLectureExample() {
    // prepare

    // create new pages

    // create page/pin with id 5

    // this should have evicted page 0

    // PIN(pageID=1)
    // PIN(pageID=2)
    // PIN(pageID=0)
    // this should have evicted page 3
  }
}
