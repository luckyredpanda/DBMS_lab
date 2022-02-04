package de.tuda.dmdb.operator.exercise;

import de.tuda.dmdb.access.AbstractDynamicIndex;
import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.access.RecordIdentifier;
import de.tuda.dmdb.operator.IndexScanBase;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import java.util.Iterator;

public class IndexScan<T extends AbstractSQLValue> extends IndexScanBase<T> {

  /**
   * Constructor of IndexScan
   *
   * @param table The table defining the record schema
   * @param index The index that should be used for the scan
   * @param constant An AbstractSQLValue that defines the key value that we are looking for (ie.
   *     point-lookup/selection predicate)
   */
  public IndexScan(AbstractTable table, AbstractDynamicIndex<T> index, T constant) {
    super(table, index, constant);
  }
  protected int num = -1;

  @Override
  public void open() {
    // TODO implement this method
    num = 0;
    TableScan tableScan = new TableScan(this.getTable());
    this.index.bulkLoad(tableScan);
  }

  @Override
  public AbstractRecord next() {
    // TODO implement this method
    if (num == -1)  throw new NullPointerException("Open IndexScan first");
    else {
      int i=-1;
      Iterator<RecordIdentifier> it = index.lookup(constant);
      AbstractRecord record1Cmp = index.getTable().getPrototype();
      while (i<num) {
        if (it.hasNext()) record1Cmp = table.lookup(it.next());
        else return null;
        i++;
      }
      num++;
      return record1Cmp;
    }
  }

  @Override
  public void close() {
    // TODO implement this method
    num = -1;
  }
}
