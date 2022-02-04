package de.tuda.dmdb.access;

import de.tuda.dmdb.buffer.exercise.BufferManager;
import de.tuda.dmdb.storage.AbstractPage;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;

/**
 * Index leaf
 *
 * @author cbinnig
 */
public abstract class LeafBase<T extends AbstractSQLValue> extends AbstractIndexElement<T> {
  /** */

  /**
   * Leaf constructor
   *
   * @param uniqueBPlusTree TODO
   */
  public LeafBase(UniqueBPlusTreeBase<T> uniqueBPlusTree) {
    super(uniqueBPlusTree);
    AbstractPage newIndexPage =
        BufferManager.getInstance()
            .createDefaultPage(uniqueBPlusTree.leafRecPrototype.getFixedLength());
    this.indexPageNumber = newIndexPage.getPageNumber();
    this.uniqueBPlusTree.addIndexElement(this);
  }

  @Override
  public boolean isOverfull() {
    return (this.getIndexPage().getNumRecords() > (uniqueBPlusTree.maxFillGrade));
  }

  @Override
  public void split(AbstractIndexElement<T> leaf1, AbstractIndexElement<T> leaf2) {
    AbstractPage thisIndexPage = getIndexPage();
    int cnt = thisIndexPage.getNumRecords();
    AbstractRecord leafRecord = this.uniqueBPlusTree.leafRecPrototype.clone();
    int slotNumber = 0;

    for (; slotNumber < cnt / 2; ++slotNumber) {
      thisIndexPage.read(slotNumber, leafRecord);
      leaf1.getIndexPage().insert(leafRecord);
    }

    for (; slotNumber < cnt; ++slotNumber) {
      thisIndexPage.read(slotNumber, leafRecord);
      leaf2.getIndexPage().insert(leafRecord);
    }
    this.uniqueBPlusTree.releaseIndexElement(this.indexPageNumber);
  }

  @Override
  public int binarySearch(T key) {
    AbstractRecord leafRecord = this.uniqueBPlusTree.leafRecPrototype.clone();
    return this.binarySearch(key, leafRecord);
  }

  @SuppressWarnings("unchecked")
  @Override
  public T getMaxKey() {
    AbstractRecord leafRecord = this.uniqueBPlusTree.leafRecPrototype.clone();
    AbstractPage indexPage = getIndexPage();
    indexPage.read(indexPage.getNumRecords() - 1, leafRecord);
    return (T) leafRecord.getValue(UniqueBPlusTreeBase.KEY_POS);
  }

  @SuppressWarnings("unchecked")
  @Override
  public T getMinKey() {
    AbstractRecord leafRecord = this.uniqueBPlusTree.leafRecPrototype.clone();
    AbstractPage indexPage = getIndexPage();
    indexPage.read(0, leafRecord);
    return (T) leafRecord.getValue(UniqueBPlusTreeBase.KEY_POS);
  }

  @Override
  protected void print(int level) {
    String indent = "";
    for (int i = 0; i < level; ++i) {
      indent += "\t";
    }

    AbstractRecord leafRecord = this.uniqueBPlusTree.leafRecPrototype.clone();

    AbstractPage indexPage = getIndexPage();
    System.out.println(indent + "Leaf:" + indexPage.getPageNumber());
    for (int i = 0; i < indexPage.getNumRecords(); ++i) {
      indexPage.read(i, leafRecord);
      System.out.println(indent + leafRecord.toString());

      SQLInteger pageNumber = (SQLInteger) leafRecord.getValue(UniqueBPlusTreeBase.PAGE_POS);
      SQLInteger slotNumber = (SQLInteger) leafRecord.getValue(UniqueBPlusTreeBase.SLOT_POS);
      System.out.println(
          indent + this.uniqueBPlusTree.table.lookup(pageNumber.getValue(), slotNumber.getValue()));
    }
    System.out.println("");
  }

  @Override
  public boolean isLeaf() {
    return true;
  }
}
