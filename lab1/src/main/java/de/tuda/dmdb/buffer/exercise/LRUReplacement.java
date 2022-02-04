package de.tuda.dmdb.buffer.exercise;

import de.tuda.dmdb.buffer.LRUReplacementBase;

/**
 * Implements the replacement strategy Last Recently Used
 *
 * @author melhindi, danfai
 */
public class LRUReplacement extends LRUReplacementBase {

  // Define here the member variables that you need to implement your algorithm
  // please only use data structures/classes available in the default java libraries
  // i.e., don't use external libraries such as Apache Common or Guava
  public Integer[] evict_id = new Integer[poolSize];
  public Integer freePool = poolSize;
  public int[] pool = new int[poolSize];
  public int evicted = 0;
  public LRUReplacement(Integer poolSize) {
    super(poolSize);
  }

  @Override
  public void fix(Integer pageId) {
    // TODO Implement this method
    System.out.println("fix " + pageId);
    System.out.println(freePool);
    for (int i=0; i<poolSize; i++)
      if (evict_id[i] == pageId) {
        for (int j=i; j<poolSize-1;j++)
          evict_id[j]=evict_id[j+1];
        evict_id[poolSize-freePool-1] = pageId;
        return;
      }
    if (freePool == 0) {
      RuntimeException e = new RuntimeException();
      throw e;
    } else {
      for (int i = 0; i < poolSize; i++)
        if (evict_id[i] == null) {
          freePool = freePool - 1;
          evict_id[i] = pageId;
          return;
        }
      unfix(evict_id[0]);
      evict_id[poolSize-1] = pageId;
    }
  }

  @Override
  public void unfix(Integer pageId) {
    // TODO Implement this method
    System.out.println("unfix "+pageId);
    for (int i=0;i<poolSize;i++)
      if (evict_id[i] == pageId){
        for (int j=i; j<poolSize-1;j++)
          evict_id[j]=evict_id[j+1];
        evict_id[poolSize-1]=null;
        evictedPages.addElement(pageId);
      }
  }

  @Override
  public Integer evict() {
    // TODO Implement this method
    Integer m = null;
    if (evictedPages.size() == evicted) {unfix(evict_id[0]);m = evictedPages.get(evicted);}
    else if (evictedPages.size() > evicted) m = evictedPages.get(evicted);
    evicted ++;
    freePool = freePool + 1;
    System.out.println("evict "+m);
    return m;
  }
}
