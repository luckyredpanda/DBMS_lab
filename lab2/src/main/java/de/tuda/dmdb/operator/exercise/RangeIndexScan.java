package de.tuda.dmdb.operator.exercise;

import de.tuda.dmdb.access.AbstractIndex;
import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.access.RecordIdentifier;
import de.tuda.dmdb.operator.RangeIndexScanBase;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import java.util.Iterator;

public class RangeIndexScan<T extends AbstractSQLValue> extends RangeIndexScanBase<T> {

  /**
   * Constructor of a RangeIndexScan with lower and upper key Lower and Upper are AbstractSQLValue
   * instances that specify the lower- and upper bound of a range. A record falls into the range if
   * {@code lower <= recordKey <= upper}.
   *
   * @param table The table defining the record schema
   * @param index The index that should be used for the scan
   * @param lower The lower bound of the range (inclusive)
   * @param upper The upper bound of the range (inclusive)
   */
  public RangeIndexScan(AbstractTable table, AbstractIndex<T> index, T lower, T upper) {
    super(table, index, lower, upper);
  }
  protected int num = -1;

  @Override
  public void open() {
    // TODO implement this method
    num = 0;
  }

  @Override
  public AbstractRecord next() {
    // TODO implement this method
    if (num == -1)  throw new NullPointerException("Open RangeIndexScan first");
    else if (!index.rangeLookup(lower,upper).hasNext()) {
      return null;
    }
    else {
      Iterator<RecordIdentifier> it = index.rangeLookup(lower,upper);
      int i=-1;
      AbstractRecord record1Cmp = index.getTable().getPrototype();
      while (i<num) {
        if (it.hasNext()) record1Cmp = index.getTable().lookup(it.next());
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
