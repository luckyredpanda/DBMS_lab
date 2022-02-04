package de.tuda.dmdb.buffer.exercise;

import de.tuda.dmdb.buffer.BufferManagerBase;
import de.tuda.dmdb.storage.AbstractPage;
import de.tuda.dmdb.storage.EnumPageType;
import de.tuda.dmdb.storage.exercise.RowPage;

import java.io.IOException;

/**
 * BufferManager maintain buffer frames
 *
 * @author melhindi
 */
public class BufferManager extends BufferManagerBase {

  /** static Singleton instance. */
  private static volatile BufferManager instance;

  private BufferManager() {
    super();
    switch (REPLACEMENT_POLICY) {
      case ClockReplacement:
        this.replacer = new ClockReplacement(POOL_SIZE);
        break;

      default:
        break;
    }
  }

  /**
   * Returns a singleton instance of BufferManager.
   *
   * @return singleton instance of BufferManager
   */
  public static BufferManagerBase getInstance() {
    // Double lock for thread safety.
    if (instance == null) {
      synchronized (BufferManager.class) {
        if (instance == null) {
          instance = new BufferManager();
        }
      }
    }
    return instance;
  }

  /** Enables creating a fresh BufferManager for tests */
  public static void clearInstance() {
    nextPageNumber = 0;
    POOL_SIZE = 1000;
    instance = null;
  }

  @Override
  public AbstractPage pin(Integer pageId) {
    // TODO implement this method
    AbstractPage page= null;
    byte[] data = null;
    if (pageTable.containsKey(pageId)){
      instance.getReplacer().fix(pageId);
      return pageTable.get(pageId);
    }
    else {
      try {
        data = diskManager.readPage(pageId);
        page= new RowPage(data);
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }
    System.out.println("frames: "+instance.freeFrames);
    if (instance.freeFrames == 0) {
      Integer evicted = instance.getReplacer().evict();
      pageTable.remove(evicted);
      instance.getReplacer().fix(pageId);
      pageTable.put(pageId,page);
    }
    else {
      pageTable.put(pageId, page);
      instance.getReplacer().fix(pageId);
      freeFrames = freeFrames - 1;
    }
    return page;
  }

  @Override
  public void unpin(Integer pageId) {
    // TODO implement this method
    System.out.println("unpin "+pageId);

    AbstractPage page = pageTable.get(pageId);
    if (pageTable.containsKey(pageId)) {
      try {
        diskManager.writePage(pageId, page.serialize());
      } catch (IOException e) {
        e.printStackTrace();
      }
      instance.getReplacer().unfix(pageId);
    }

  }

  @Override
  public AbstractPage createPage(EnumPageType type, byte[] data) {
    // TODO implement this method
    AbstractPage page = null;
    switch (type) {
      case RowPageType:
        page = new RowPage(data);
        break;
      default:
        throw new IllegalArgumentException("You passed an unsupported page type");
    }
    try {
      diskManager.writePage(page.getPageNumber(),page.serialize());
    } catch (IOException e) {
      e.printStackTrace();
    }
    page = pin(page.getPageNumber());
    return page;
  }

  @Override
  public AbstractPage createDefaultPage(EnumPageType type, int slotSize) {
    // TODO implement this method
    AbstractPage page = null;
    switch (type) {
      case RowPageType:
        page = new RowPage(slotSize);
        break;
      default:
        throw new IllegalArgumentException("You passed an unsupported page type");
    }
    page.setPageNumber(diskManager.getNextPageId());
    try {
      diskManager.writePage(page.getPageNumber(),page.serialize());
    } catch (IOException e) {
      e.printStackTrace();
    }
    page = pin(page.getPageNumber());
    return page;
  }
}
