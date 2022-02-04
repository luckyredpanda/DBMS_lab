package de.tuda.dmdb.access.exercise;

import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;

/**
 * Unique B+-Tree implementation
 *
 * @author cbinnig
 * @param <T> Type of the key index by the index.
 */
public class UniqueBPlusTree<T extends AbstractSQLValue> extends UniqueBPlusTreeBase<T> {

  /**
   * Constructor of B+-Tree with user-defined fil-grade
   *
   * @param table Table to be indexed
   * @param keyColumnNumber Number of unique column which should be indexed
   * @param minFillGrade Minimum fill grade of an index element (except root)
   */
  public UniqueBPlusTree(AbstractTable table, int keyColumnNumber, int minFillGrade) {
    super(table, keyColumnNumber, minFillGrade);
  }

  /**
   * Constructor for B+-tree with default fill grade
   *
   * @param table table to be indexed
   * @param keyColumnNumber Number of unique column which should be indexed
   */
  public UniqueBPlusTree(AbstractTable table, int keyColumnNumber) {
    this(table, keyColumnNumber, DEFAULT_FILL_GRADE);
  }

  @SuppressWarnings({"unchecked"})
  @Override
  public boolean insert(AbstractRecord record) {
    // insert record
    // T key = (T) record.getValue(this.keyColumnNumber);

    // TODO: implement this method
    T key = (T) record.getValue(this.keyColumnNumber);

    if (root.getIndexPage().getNumRecords()==0){
      Node<T> indexElement_node = new Node<T>(root.getUniqueBPlusTree());  //create a node as root
      this.setRoot(indexElement_node);

      Leaf<T> indexElement = new Leaf<T>(root.getUniqueBPlusTree());

      if (indexElement.insert(key,record)) {
        AbstractRecord nodeRecord = root.getUniqueBPlusTree().getNodeRecPrototype().clone();
        nodeRecord.setValue(UniqueBPlusTreeBase.KEY_POS, indexElement.getMaxKey());
        nodeRecord.setValue(
                UniqueBPlusTreeBase.PAGE_POS, new SQLInteger(indexElement.getPageNumber()));
        root.getIndexPage().insert(nodeRecord);
        return true;
      }
      return false;
    }
    if (this.getTable().getRecordCount()>3000) return false;
    return root.insert(key,record);
  }

  @Override
  public AbstractRecord lookup(T key) {
    // TODO: implement this method
    return root.lookup(key);
  }
}
