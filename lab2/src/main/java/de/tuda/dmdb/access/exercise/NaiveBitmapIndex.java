package de.tuda.dmdb.access.exercise;

import de.tuda.dmdb.access.AbstractBitmapIndex;
import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.access.RecordIdentifier;
import de.tuda.dmdb.operator.TableScanBase;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Bitmap index that uses the vanilla/naive bitmap approach (one bitmap for each distinct value)
 *
 * @author melhindi
 * @param <T> Type of the key index by the index. While all abstractSQLValues subclasses can be
 *     used, the implementation currently only support for SQLInteger type is guaranteed.
 */
public class NaiveBitmapIndex<T extends AbstractSQLValue> extends AbstractBitmapIndex<T> {

  /*
   * Constructor of NaiveBitmapIndex
   *
   * @param table Table for which the bitmap index will be build
   *
   * @param keyColumnNumber: index of the column within the passed table that
   * should be indexed
   */
  public NaiveBitmapIndex(AbstractTable table, int keyColumnNumber) {
    super(table, keyColumnNumber);
    this.bitMaps = new HashMap<T, BitSet>();
    this.bitmapSize = this.getTable().getRecordCount();
    TableScan tableScan = new TableScan(this.getTable());
    this.bulkLoad(tableScan);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void bulkLoad(TableScanBase tableScan) {
    // TODO Implement this method
    tableScan.open();
    int m = 0;
    do {
      BitSet b = new BitSet(bitmapSize);
      AbstractRecord r = tableScan.next();
      T key = (T) r.getValue(keyColumnNumber);
      if (bitMaps.containsKey(key)) b = bitMaps.get(key);
      b.set(m);
      bitMaps.put(key,b);
      m++;
    }while (m<bitmapSize);
    tableScan.close();
  }

  @Override
  public Iterator<RecordIdentifier> rangeLookup(T startKey, T endKey) {
    // TODO Implement this method
    ArrayList l = new ArrayList();
    for (T key : bitMaps.keySet())
      if (key.compareTo(startKey)>=0 && key.compareTo(endKey)<=0){
        for (int i = 0;i< bitMaps.get(key).cardinality();i++){
          l.add(table.getRecordIDFromRowNum(bitMaps.get(key).nextSetBit(i)));
        }
      }
    Iterator it = l.iterator();
    return it;
  }

  @Override
  public boolean isUniqueIndex() {
    return false;
  }
}
