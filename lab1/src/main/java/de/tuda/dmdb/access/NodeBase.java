package de.tuda.dmdb.access;

import de.tuda.dmdb.buffer.exercise.BufferManager;
import de.tuda.dmdb.storage.AbstractPage;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import java.util.Vector;

/**
 * Index node
 *
 * @author cbinnig
 */
public abstract class NodeBase<T extends AbstractSQLValue> extends AbstractIndexElement<T> {

  /**
   * Node constructor
   *
   * @param uniqueBPlusTree TODO
   */
  public NodeBase(UniqueBPlusTreeBase<T> uniqueBPlusTree) {
    super(uniqueBPlusTree);
    AbstractPage newIndexPage =
        BufferManager.getInstance()
            .createDefaultPage(this.uniqueBPlusTree.nodeRecPrototype.getFixedLength());
    this.indexPageNumber = newIndexPage.getPageNumber();
    this.uniqueBPlusTree.addIndexElement(this);
  }

  @Override
  public boolean isOverfull() {
    return (this.getIndexPage().getNumRecords() > (uniqueBPlusTree.maxFillGrade + 1));
  }

  @Override
  public void split(AbstractIndexElement<T> node1, AbstractIndexElement<T> node2) {
    AbstractPage thisIndexPage = getIndexPage();
    int cnt = thisIndexPage.getNumRecords();
    AbstractRecord nodeRecord = this.uniqueBPlusTree.nodeRecPrototype.clone();
    int slotNumber = 0;

    for (; slotNumber < cnt / 2; ++slotNumber) {
      thisIndexPage.read(slotNumber, nodeRecord);
      node1.getIndexPage().insert(nodeRecord);
    }

    for (; slotNumber < cnt; ++slotNumber) {
      thisIndexPage.read(slotNumber, nodeRecord);
      node2.getIndexPage().insert(nodeRecord);
    }
    this.uniqueBPlusTree.releaseIndexElement(thisIndexPage.getPageNumber());
  }

  @Override
  public int binarySearch(T key) {
    AbstractRecord nodeRecord = this.uniqueBPlusTree.nodeRecPrototype.clone();
    int index = this.binarySearch(key, nodeRecord);
    int last = this.getIndexPage().getNumRecords() - 1;
    return (index < last) ? index : last;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T getMaxKey() {
    AbstractRecord nodeRecord = this.uniqueBPlusTree.nodeRecPrototype.clone();
    AbstractPage indexPage = getIndexPage();
    indexPage.read(indexPage.getNumRecords() - 1, nodeRecord);
    return (T) nodeRecord.getValue(UniqueBPlusTreeBase.KEY_POS);
  }

  @Override
  protected void print(int level) {
    String indent = "";
    for (int i = 0; i < level; ++i) {
      indent += "\t";
    }

    Vector<AbstractIndexElement<T>> children = new Vector<AbstractIndexElement<T>>();
    AbstractRecord nodeRecord = this.uniqueBPlusTree.nodeRecPrototype.clone();

    AbstractPage indexPage = getIndexPage();
    System.out.println(indent + "Node:" + indexPage.getPageNumber());
    for (int i = 0; i < indexPage.getNumRecords(); ++i) {
      indexPage.read(i, nodeRecord);
      System.out.println(indent + nodeRecord.toString());

      SQLInteger pageNumber = (SQLInteger) nodeRecord.getValue(UniqueBPlusTreeBase.PAGE_POS);
      children.add(this.uniqueBPlusTree.getIndexElement(pageNumber.getValue()));
    }
    System.out.println("");

    for (AbstractIndexElement<T> child : children) {
      child.print(level + 1);
    }
  }
}
