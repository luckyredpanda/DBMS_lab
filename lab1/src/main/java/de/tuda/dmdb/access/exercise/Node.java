package de.tuda.dmdb.access.exercise;

import de.tuda.dmdb.access.AbstractIndexElement;
import de.tuda.dmdb.access.NodeBase;
import de.tuda.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;

/**
 * Index node
 *
 * @author cbinnig
 */
public class Node<T extends AbstractSQLValue> extends NodeBase<T> {

  /**
   * Node constructor
   *
   * @param uniqueBPlusTree TODO
   */
  public Node(UniqueBPlusTreeBase<T> uniqueBPlusTree) {
    super(uniqueBPlusTree);
  }

  @Override
  public AbstractRecord lookup(T key) {
    // TODO: implement this method
    AbstractRecord record_c = this.getUniqueBPlusTree().getNodeRecPrototype().clone();
    int m = this.binarySearch(key);
    this.getIndexPage().read(m, record_c);
    int num_page = record_c.getValue(1).compareTo(new SQLInteger(0));
    AbstractIndexElement<T> node = this.getUniqueBPlusTree().getIndexElement(num_page);
    return node.lookup(key);
  }

  @Override
  public boolean insert(T key, AbstractRecord record) {
    // TODO: implement this method
    int m = this.binarySearch(key);
    AbstractRecord nodeRecord = this.getUniqueBPlusTree().getNodeRecPrototype().clone();
    this.getIndexPage().read(m, nodeRecord);
    int num_page = nodeRecord.getValue(1).compareTo( new SQLInteger(0));
    AbstractIndexElement<T> indexElement = this.getUniqueBPlusTree().getIndexElement(num_page);
    if (indexElement.insert(key, record)) {
      AbstractIndexElement<T> indexElement3 = this;
      if (indexElement.isOverfull()) {
        AbstractIndexElement<T> indexElement1 = indexElement.createInstance();
        AbstractIndexElement<T> indexElement2 = indexElement.createInstance();
        indexElement.split(indexElement1, indexElement2);
        nodeRecord.setValue(UniqueBPlusTreeBase.KEY_POS, indexElement1.getMaxKey());
        nodeRecord.setValue(
                UniqueBPlusTreeBase.PAGE_POS, new SQLInteger(indexElement1.getPageNumber()));
        indexElement3.getIndexPage().insert(m, nodeRecord, false);
        nodeRecord.setValue(UniqueBPlusTreeBase.KEY_POS, indexElement2.getMaxKey());
        nodeRecord.setValue(
                UniqueBPlusTreeBase.PAGE_POS, new SQLInteger(indexElement2.getPageNumber()));
        indexElement3.getIndexPage().insert(m+1, nodeRecord, true);
        if (indexElement3.isOverfull()) {
          indexElement1 = indexElement3.createInstance();
          indexElement2 = indexElement3.createInstance();
          indexElement3.split(indexElement1, indexElement2);
          indexElement3 = new Node<T>(this.getUniqueBPlusTree());
          nodeRecord.setValue(UniqueBPlusTreeBase.KEY_POS, indexElement1.getMaxKey());
          nodeRecord.setValue(
                  UniqueBPlusTreeBase.PAGE_POS, new SQLInteger(indexElement1.getPageNumber()));
          indexElement3.getIndexPage().insert(nodeRecord);
          nodeRecord.setValue(UniqueBPlusTreeBase.KEY_POS, indexElement2.getMaxKey());
          nodeRecord.setValue(
                  UniqueBPlusTreeBase.PAGE_POS, new SQLInteger(indexElement2.getPageNumber()));
          indexElement3.getIndexPage().insert(nodeRecord);
          indexElement3.getIndexPage().setPageNumber(this.getPageNumber());
          if (this == this.getUniqueBPlusTree().getRoot()) this.getUniqueBPlusTree().setRoot(indexElement3);
        }
      }
      return true;
    }
    return false;

  }

  @Override
  public AbstractIndexElement<T> createInstance() {
    return new Node<T>(this.uniqueBPlusTree);
  }
}
