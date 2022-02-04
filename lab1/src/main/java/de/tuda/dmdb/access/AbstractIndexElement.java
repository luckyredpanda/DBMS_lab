package de.tuda.dmdb.access;

import de.tuda.dmdb.buffer.BufferManagerBase;
import de.tuda.dmdb.buffer.exercise.BufferManager;
import de.tuda.dmdb.storage.AbstractPage;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;

/**
 * Abstract class for an index element (node or leaf)
 *
 * @author cbinnig
 */
public abstract class AbstractIndexElement<T extends AbstractSQLValue> {
  /** */
  protected UniqueBPlusTreeBase<T> uniqueBPlusTree;

  // Use BufferManager to get AbstractPages from pin()
  protected BufferManagerBase bufferManager = BufferManager.getInstance();

  /** @param uniqueBPlusTree */
  AbstractIndexElement(UniqueBPlusTreeBase<T> uniqueBPlusTree) {
    this.uniqueBPlusTree = uniqueBPlusTree;
  }

  // page-number index page to store index entries
  protected int indexPageNumber;

  /**
   * Checks if index page exceeds max. fill grade
   *
   * @return true if the index page exceeds max. fill grade
   */
  public abstract boolean isOverfull();

  /**
   * creates a leaf or a node depending on type of object
   *
   * @return an empty object of itself
   */
  public abstract AbstractIndexElement<T> createInstance();

  /**
   * lookup a record for a key in index
   *
   * @param key the key to lookup
   * @return a record matching the key
   */
  public abstract AbstractRecord lookup(T key);

  /**
   * Inserts a new record into index
   *
   * @param key Key of the AbstractRecord, ie. the value in the indexed column
   * @param record AbstractRecord that should be inserted
   * @return True if record could be inserted, false if key is already used
   */
  public abstract boolean insert(T key, AbstractRecord record);

  /**
   * Get maximum key value of index element
   *
   * @return the maximum key value of this index element
   */
  public abstract T getMaxKey();

  /**
   * Split index element into two elements
   *
   * @param element1 Reference to first element
   * @param element2 Reference to second element
   */
  public abstract void split(AbstractIndexElement<T> element1, AbstractIndexElement<T> element2);

  /**
   * Binary search for key
   *
   * @param key the key to search for
   * @return Slot number of node entry
   */
  public abstract int binarySearch(T key);

  /**
   * Print index elements
   *
   * @param level depth of this index element used for indention
   */
  protected abstract void print(int level);

  /** Print index elements */
  public void print() {
    this.print(0);
  }

  /**
   * Returns page number of index element
   *
   * @return page number of this index element
   */
  public int getPageNumber() {
    return this.indexPageNumber;
  }

  /**
   * Binary search on index element. If element is not found then it returns pointer to next bigger
   * element in index element or points to slot after last element if key is bigger than all index
   * element entries.
   *
   * @param key lookup key
   * @param indexRecord record to read from index element (node or leaf)
   * @return slot number of node entry
   */
  protected int binarySearch(T key, AbstractRecord indexRecord) {
    AbstractPage indexPage = getIndexPage();
    int end = indexPage.getNumRecords() - 1;
    int start = 0;
    int center = -1;

    while (start <= end) {
      center = (start + end) / 2;
      indexPage.read(center, indexRecord);
      AbstractSQLValue keyValue = indexRecord.getValue(UniqueBPlusTreeBase.KEY_POS);

      if (key.compareTo(keyValue) == 0) {
        return center;
      } else if (key.compareTo(keyValue) < 0) {
        end = center - 1;
      } else if (key.compareTo(keyValue) > 0) {
        start = center + 1;
      }
    }
    return start;
  }

  public AbstractPage getIndexPage() {
    return bufferManager.pin(this.indexPageNumber);
  }

  public UniqueBPlusTreeBase<T> getUniqueBPlusTree() {
    return uniqueBPlusTree;
  }

  public void setUniqueBPlusTree(UniqueBPlusTreeBase<T> uniqueBPlusTree) {
    this.uniqueBPlusTree = uniqueBPlusTree;
  }
}
