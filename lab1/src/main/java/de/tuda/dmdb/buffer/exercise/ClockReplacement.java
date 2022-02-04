package de.tuda.dmdb.buffer.exercise;

import de.tuda.dmdb.buffer.ClockReplacementBase;

import java.util.Vector;

/**
 * Implements the clock replacement strategy
 *
 * @author melhindi
 */
public class ClockReplacement extends ClockReplacementBase {

  // Define here the member variables that you need to implement your algorithm
  // please only use data structures/classes available in the default java libraries
  // i.e., don't use external libraries such as Apache Common or Guava
  public Integer[] evict_id = new Integer[poolSize];
  public Integer freePool = poolSize;
  public int[] pool = new int[poolSize];
  public int evicted = 0;
  public Vector<Integer>
          p_evictedPages = new Vector<Integer>();
  public ClockReplacement(Integer poolSize) {
    super(poolSize);
  }

  @Override
  public void fix(Integer pageId) {
    // TODO Implement this method
    for (int i=0;i<poolSize;i++) {
      if (evict_id[i] == pageId) {
        pool[i] = 1;
        p_evictedPages.remove(pageId);
        clockHandPos = i;
        return;
      }
    }
    if (freePool == 0) {
      RuntimeException e = new RuntimeException();
      throw e;
    } else {
      if (pool[clockHandPos] == 0) {
        evict_id[clockHandPos] = pageId;
        pool[clockHandPos] = 1;
        clockHandPos = (clockHandPos + 1) % poolSize;
        freePool = freePool - 1;
      } else {
        while (pool[clockHandPos] != 0)
          {
            unfix(evict_id[clockHandPos]);
          }
        evict_id[clockHandPos] = pageId;
        pool[clockHandPos] = 1;
      }
    }
  }

  @Override
  public void unfix(Integer pageId) {
    // TODO Implement this method
    System.out.println("unfix "+pageId);
    for (int i=0;i<poolSize;i++) {
      if (evict_id[i] == pageId && pool[i] == 1) {
        pool[i] = 0;
        p_evictedPages.addElement(pageId);
        clockHandPos = (i + 1) % poolSize;
        return;
      } else if (evict_id[i] == pageId && pool[i] == 0) {
        pool[i] = 1;
        p_evictedPages.remove(p_evictedPages.size() - 1);
        clockHandPos = i;
        return;
      }
    }
  }

  @Override
  public Integer evict() {
    // TODO Auto-generated method stub
    Integer m = null;
    if (p_evictedPages.size() == 0) {
      unfix(evict_id[0]);
      m = p_evictedPages.get(evicted);
    }
    else if (p_evictedPages.size() > evicted) {
      m = p_evictedPages.get(evicted);
    }
    evictedPages.addElement(m);
    evicted++;
    freePool = freePool + 1;
    return m;
  }
}
