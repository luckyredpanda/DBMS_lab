package de.tuda.dmdb.storage;

/**
 * @author melhindi, danfai
 *     <p>The DiskManager takes care of the allocation and deallocation of pages within a database.
 *     It performs the reading and writing of pages to and from disk, providing a logical file layer
 *     within the context of a database management system.
 */
public abstract class AbstractDiskManager {

  /** File name of the database */
  public static final String DB_FILENAME = "testdb.sdms";

  // The actual file handle should be created by students in the exercise class

  /** Close File descriptor and make sure everything in consistent state. */
  public abstract void close();

  /**
   * Reads the page from Disk and returns it.
   *
   * @param pageId the ID of the page to be read.
   * @return AbstractPage the page or null if not found
   */
  public abstract AbstractPage readPage(Integer pageId);

  /**
   * Writes a given page to disk.
   *
   * @param page the page to be written.
   */
  public abstract void writePage(AbstractPage page);
}
