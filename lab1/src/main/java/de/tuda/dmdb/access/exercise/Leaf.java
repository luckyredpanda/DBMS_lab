package de.tuda.dmdb.access.exercise;

import de.tuda.dmdb.access.AbstractIndexElement;
import de.tuda.dmdb.access.LeafBase;
import de.tuda.dmdb.access.RecordIdentifier;
import de.tuda.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;

/**
 * Index leaf
 *
 * @author cbinnig Note: Leaf-level pointers omitted since AbstractUniqueIndex only supports
 *     single-key lookup
 */
public class Leaf<T extends AbstractSQLValue> extends LeafBase<T> {

  /**
   * Leaf constructor
   *
   * @param uniqueBPlusTree TODO
   */
  public Leaf(UniqueBPlusTreeBase<T> uniqueBPlusTree) {
    super(uniqueBPlusTree);
  }

  @Override
  public AbstractRecord lookup(T key) {
    // TODO: implement this method
    for (int i=0;i<this.getIndexPage().getNumRecords();i++) {
      AbstractRecord r = this.getUniqueBPlusTree().getLeafRecPrototype().clone();
      this.getIndexPage().read(i, r);
      RecordIdentifier m = null;
      if (r.getValue(0).equals(key)) {
        int page = r.getValue(1).compareTo(new SQLInteger(0));
        int slot = r.getValue(2).compareTo(new SQLInteger(0));
//        uniqueBPlusTree.getIndexElement(1).getIndexPage().read(slot, r);
        r = uniqueBPlusTree.getTable().lookup(page,slot);
        return r;
      }
    }
    return null;
  }

  @Override
  public boolean insert(T key, AbstractRecord record) {
    // TODO: implement this method
    for (int i=0;i<uniqueBPlusTree.getTable().getRecordCount();i++)
      if (uniqueBPlusTree.getTable().getRecordFromRowId(i).equals(record)) return false;
    RecordIdentifier m;
    m = uniqueBPlusTree.getTable().insert(record);
    AbstractRecord r2 = new Record(3);
    r2.setValue(0,key);
    r2.setValue(1,new SQLInteger(m.getPageNumber()));
    r2.setValue(2,new SQLInteger(m.getSlotNumber()));
    if (this.getIndexPage().getNumRecords() == 0 ) {
      this.getIndexPage().insert(r2);
      return true;
    }
    else {
      for (int i = 0; i < this.getIndexPage().getNumRecords(); i++) {
        AbstractRecord r1 = this.getUniqueBPlusTree().getLeafRecPrototype().clone();
        this.getIndexPage().read(i, r1);
        if (r1.getValue(0).compareTo(r2.getValue(0)) > 0) {
          this.getIndexPage().insert(i, r2, true);
          return true;
        }
      }
      this.getIndexPage().insert(r2);
    }
    return true;
  }

  @Override
  public AbstractIndexElement<T> createInstance() {
    return new Leaf<T>(this.uniqueBPlusTree);
  }
}
