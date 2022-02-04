package de.tuda.dmdb.buffer.advanced;

import org.junit.jupiter.api.Test;

public class TestAdvancedLRUReplacement {

  /** Test that evict() frees a frame for a subsequent fix() operations */
  @Test
  public void testFixAfterEviction() {
    // fill clock to capacity

    // try fixing a new page - expect Exception

    // fixing a new page should be possible after eviction
  }

  /** Test that unfixing a page will make it a victim for the next eviction */
  @Test
  public void testUnfix() {
    // fill clock to capacity

    // unfix a random page

    // calling evict should evict the previously unfixed page first
  }

  /** Test that unfixing a page will make it a victim for the next eviction */
  @Test
  public void testEvictingFixedPagesAfterUnfixedPages() {
    // fill clock to capacity

    // unfix a random page

    // calling evict should evict the previously unfixed page first

    // evicting all pages and check size

    // evicting should no longer be possible
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

    // unfix the previously evicted page, i.e., it is now unknown
    // we expect that simply nothing will happen
    // some students might throw an exception which is ok
  }
}
