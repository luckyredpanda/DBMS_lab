package de.tuda.dmdb.storage;

import java.io.IOException;

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
   * Reads the page from Disk by its ID and return its data. If the page cannot be found on disk,
   * return null.
   *
   * @param pageId The ID of the page to be read from disk.
   * @return The page data as byte array or null if page could not be found
   * @throws IOException if I/O error occurs
   */
  public abstract byte[] readPage(int pageId) throws IOException;

  /**
   * Writes a given page to disk. Note that - e.g., due to concurrency - a write(1,...) could happen
   * before write(0, ...)
   *
   * @param pageId the page to be written.
   * @param pageData data of page as byte array
   * @throws IOException when pageId or pageData are invalid
   */
  public abstract void writePage(int pageId, byte[] pageData) throws IOException;

  /**
   * Determine the next available (free) pageId that can be assigned to a new page This method
   * should be thread-safe such that two concurrent threads that request a pageId are not assigned
   * the same pageId
   *
   * @return The unique pageId that should be assigned to a new page
   */
  public abstract int getNextPageId();
}
