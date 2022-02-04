package de.tuda.dmdb.access;

import de.tuda.dmdb.access.exercise.Leaf;
import de.tuda.dmdb.buffer.exercise.BufferManager;
import de.tuda.dmdb.storage.AbstractPage;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import java.util.HashMap;

/**
 * Unique B+-Tree implementation
 *
 * @author cbinnig
 * @param <T> Note: Leaf-level pointers omitted since AbstractUniqueIndex only supports single-key
 *     lookup
 */
public abstract class UniqueBPlusTreeBase<T extends AbstractSQLValue>
    extends AbstractUniqueIndex<T> {
  // constants
  public static final int KEY_POS = 0;
  public static final int PAGE_POS = 1;
  public static final int SLOT_POS = 2;
  public static final int DEFAULT_FILL_GRADE = 100;

  protected int keyColumnNumber; // column number of key attribute in table
  protected int maxFillGrade; // max. fill grade

  protected AbstractRecord nodeRecPrototype; // node record: [keyValue: T, pageNumber: SQLInteger]
  protected AbstractRecord
      leafRecPrototype; // leaf record: [keyValue: T, pageNumber: SQLInteger, slot number:
  // SQLInteger]

  protected AbstractIndexElement<T> root; // root index element

  protected HashMap<Integer, AbstractIndexElement<T>> indexElements =
      new HashMap<Integer, AbstractIndexElement<T>>(); // map (pageNumber, indexElement)

  /**
   * Constructor of B+-Tree with user-defined fil-grade
   *
   * @param table Table to be indexed
   * @param keyColumnNumber Number of unique column which should be indexed
   * @param minFillGrade Minimum fill grade of an index element (except root)
   */
  public UniqueBPlusTreeBase(AbstractTable table, int keyColumnNumber, int minFillGrade) {
    super(table);

    // set index properties
    this.keyColumnNumber = keyColumnNumber;
    this.maxFillGrade = minFillGrade * 2;

    // attributes of index records
    AbstractSQLValue keyValue = this.table.getPrototype().getValue(keyColumnNumber);
    SQLInteger pageNumber = new SQLInteger();
    SQLInteger slotNumber = new SQLInteger();

    // prototype for node record
    this.nodeRecPrototype = new Record(2);
    this.nodeRecPrototype.setValue(KEY_POS, keyValue);
    this.nodeRecPrototype.setValue(PAGE_POS, pageNumber);

    // prototype for leaf record
    this.leafRecPrototype = new Record(3);
    this.leafRecPrototype.setValue(KEY_POS, keyValue);
    this.leafRecPrototype.setValue(PAGE_POS, pageNumber);
    this.leafRecPrototype.setValue(SLOT_POS, slotNumber);

    // create root node
    this.root = new Leaf<T>(this);

    // check fill grade
    if ((this.maxFillGrade + 1) > AbstractPage.estimateRecords(this.leafRecPrototype)) {
      throw new IllegalArgumentException(
          "Fill grade exceeds number of records that can be stored in a index node!");
    }
  }

  /**
   * Constructor for B+-tree with default fill grade
   *
   * @param table table to be indexed
   * @param keyColumnNumber Number of unique column which should be indexed
   */
  public UniqueBPlusTreeBase(AbstractTable table, int keyColumnNumber) {
    this(table, keyColumnNumber, DEFAULT_FILL_GRADE);
  }

  /**
   * Add index element to list of index elements in index
   *
   * @param indexElement page to be added
   */
  void addIndexElement(AbstractIndexElement<T> indexElement) {
    ;
    this.indexElements.put(indexElement.getPageNumber(), indexElement);
  }

  /**
   * Get index element by page number
   *
   * @param pageNumber the page number of the index to be received
   * @return the index page belonging to the page number
   */
  public AbstractIndexElement<T> getIndexElement(int pageNumber) {
    return this.indexElements.get(pageNumber);
  }

  /**
   * Remove index element by page number
   *
   * @param pageNumber the page number to be released
   */
  void releaseIndexElement(int pageNumber) {
    BufferManager.getInstance().unpin(pageNumber);
    this.indexElements.remove(pageNumber);
  }

  @Override
  public void print() {
    this.root.print();
  }

  public int getKeyColumnNumber() {
    return keyColumnNumber;
  }

  public void setKeyColumnNumber(int keyColumnNumber) {
    this.keyColumnNumber = keyColumnNumber;
  }

  public int getMaxFillGrade() {
    return maxFillGrade;
  }

  public void setMaxFillGrade(int maxFillGrade) {
    this.maxFillGrade = maxFillGrade;
  }

  public AbstractRecord getNodeRecPrototype() {
    return nodeRecPrototype;
  }

  public void setNodeRecPrototype(AbstractRecord nodeRecPrototype) {
    this.nodeRecPrototype = nodeRecPrototype;
  }

  public AbstractRecord getLeafRecPrototype() {
    return leafRecPrototype;
  }

  public void setLeafRecPrototype(AbstractRecord leafRecPrototype) {
    this.leafRecPrototype = leafRecPrototype;
  }

  public AbstractIndexElement<T> getRoot() {
    return root;
  }

  public void setRoot(AbstractIndexElement<T> root) {
    this.root = root;
  }

  public HashMap<Integer, AbstractIndexElement<T>> getIndexElements() {
    return indexElements;
  }

  public void setIndexElements(HashMap<Integer, AbstractIndexElement<T>> indexElements) {
    this.indexElements = indexElements;
  }
}
