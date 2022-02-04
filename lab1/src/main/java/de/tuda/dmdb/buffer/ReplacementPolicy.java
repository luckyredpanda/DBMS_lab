package de.tuda.dmdb.buffer;

import java.util.Vector;

/**
 * Interface that defines how replacement of buffer frames is done A ReplacementStrategy should
 * maintain which pages are pinned or unpinned in the buffer pool Evicted pages should be logged in
 * the evictedPages member variable for testing purposes
 *
 * @author melhindi
 */
public abstract class ReplacementPolicy {
  protected int poolSize; // max available frames
  protected Vector<Integer>
      evictedPages; // use this member for logging evicted pages (for testing purposes)

  public ReplacementPolicy(int poolSize) {
    this.poolSize = poolSize;
    this.evictedPages = new Vector<Integer>();
  }

  /**
   * Fixes a frame, indicating that it should not be evicted until it is unfixed. Think: set the
   * refBit of page in the clock algorithm
   *
   * @param pageId the id of the frame to pin
   * @throws RuntimeException when it is not possible to fix the page, e.g. no free frame is
   *     available
   */
  public abstract void fix(Integer pageId);

  /**
   * Unfixes a frame, indicating that it could be evicted. E.g.: unset the refBit in the clock
   * algorithm Unfixing a previously evicted or unknown page should do nothing (there is nothing to
   * unfix)
   *
   * @param pageId the id of the frame to unpin
   */
  public abstract void unfix(Integer pageId);

  /**
   * Determine the page that should be evicted next from the buffer
   *
   * @return pageId of victim to evict or null if there should be free frames, e.g. no need to move
   *     clockhand
   */
  public abstract Integer evict();

  /*
   * Getter- and Setter-Methods
   */

  public int getPoolSize() {
    return poolSize;
  }

  public void setPoolSize(int poolSize) {
    this.poolSize = poolSize;
  }

  public Vector<Integer> getEvictedPages() {
    return evictedPages;
  }

  public void setEvictedPages(Vector<Integer> evictedPages) {
    this.evictedPages = evictedPages;
  }
}
