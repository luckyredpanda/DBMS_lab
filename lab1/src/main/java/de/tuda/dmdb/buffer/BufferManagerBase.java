package de.tuda.dmdb.buffer;

import de.tuda.dmdb.storage.AbstractDiskManager;
import de.tuda.dmdb.storage.AbstractPage;
import de.tuda.dmdb.storage.EnumPageType;
import de.tuda.dmdb.storage.exercise.DiskManager;
import java.util.HashMap;

public abstract class BufferManagerBase {
  protected static int nextPageNumber = 0; // counter for unique page numbers in table
  public static int POOL_SIZE = 1000; // think of this as a config parameter
  public static EnumReplacementPolicy REPLACEMENT_POLICY = EnumReplacementPolicy.ClockReplacement;

  protected Integer bufferPoolSize; // available main memory for pages
  protected Integer freeFrames; // counts number of free buffer frames
  protected HashMap<Integer, AbstractPage>
      pageTable; // keeps references to page data, pageId -> page
  protected AbstractDiskManager diskManager; // used to persist pages
  protected ReplacementPolicy replacer; // used to determine pages to evict

  protected BufferManagerBase() {
    this.bufferPoolSize = POOL_SIZE;
    this.freeFrames = this.bufferPoolSize;
    this.diskManager = new DiskManager();
    this.pageTable = new HashMap<Integer, AbstractPage>();
  }

  /**
   * Pin a page in the buffer pool Load the page from disk if not already available in the buffer
   *
   * @param pageId of page to pin
   * @return reference to page or null if page cannot be pinned, e.g., if buffer is full
   */
  public abstract AbstractPage pin(Integer pageId);

  /**
   * Unpin page in the buffer pool Unpinned pages will be evicted later if required
   *
   * @param pageId id of page to unpin
   */
  public abstract void unpin(Integer pageId);

  /**
   * Creates a page based on its passed serialized data. The pageId of the page is inferred from the
   * serialized data
   *
   * @param type The type of the page to create
   * @param data The serialized data of the page that should be deserialized to re-create the page
   * @return the created page
   */
  public abstract AbstractPage createPage(EnumPageType type, byte[] data);

  /**
   * Creates a page with a default page type and from the passed serialized data. The pageId of the
   * page is inferred from the serialized data
   *
   * @param data The serialized data of the page that should be deserialized to re-create the page
   * @return the created page
   */
  public AbstractPage createPage(byte[] data) {
    return createPage(EnumPageType.RowPageType, data);
  }

  /**
   * Creates a page for a given page type and slot size. A new pageId for the newly created page
   * must be requested from the diskManager
   *
   * @param type Enum PageType
   * @param slotSize Size of lot in bytes
   * @return the created page
   */
  public abstract AbstractPage createDefaultPage(EnumPageType type, int slotSize);

  /**
   * Creates a page with a default page type and a given slot size. A new pageId for the newly
   * created page must be requested from the diskManager
   *
   * @param slotSize size of lot in bytes
   * @return the created page
   */
  public AbstractPage createDefaultPage(int slotSize) {
    return createDefaultPage(EnumPageType.RowPageType, slotSize);
  }

  /*
   * Getter and Setter methods, mainly used during testing
   */

  /** @return the bufferPoolSize */
  public Integer getBufferPoolSize() {
    return bufferPoolSize;
  }

  /** @param bufferPoolSize the bufferPoolSize to set */
  public void setBufferPoolSize(Integer bufferPoolSize) {
    this.bufferPoolSize = bufferPoolSize;
  }

  /** @return the freeBuffers */
  public Integer getFreeFrames() {
    return freeFrames;
  }

  /** @param freeBuffers the freeBuffers to set */
  public void setFreeFrames(Integer freeBuffers) {
    this.freeFrames = freeBuffers;
  }

  /** @return the pageTable */
  public HashMap<Integer, AbstractPage> getPageTable() {
    return pageTable;
  }

  /** @param pageTable the pageTable to set */
  public void setPageTable(HashMap<Integer, AbstractPage> pageTable) {
    this.pageTable = pageTable;
  }

  /** @return the diskManager */
  public AbstractDiskManager getDiskManager() {
    return diskManager;
  }

  /** @param diskManager the diskManager to set */
  public void setDiskManager(AbstractDiskManager diskManager) {
    this.diskManager = diskManager;
  }

  /** @return the replacer */
  public ReplacementPolicy getReplacer() {
    return replacer;
  }

  /** @param replacer the replacer to set */
  public void setReplacer(ReplacementPolicy replacer) {
    this.replacer = replacer;
  }
}
