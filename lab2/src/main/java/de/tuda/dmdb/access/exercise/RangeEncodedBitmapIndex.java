package de.tuda.dmdb.access.exercise;

import de.tuda.dmdb.access.AbstractBitmapIndex;
import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.access.RecordIdentifier;
import de.tuda.dmdb.operator.TableScanBase;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import java.util.*;

/**
 * Bitmap index that uses the range encoded approach (still one bitmap for each distinct value)
 *
 * @author lthostrup
 * @param <T> Type of the key index by the index. While all abstractSQLValues subclasses can be
 *     used, the implementation currently only support for SQLInteger type is guaranteed.
 */
public class RangeEncodedBitmapIndex<T extends AbstractSQLValue> extends AbstractBitmapIndex<T> {

  /**
   * Constructor of NaiveBitmapIndex
   *
   * @param table Table for which the bitmap index will be build
   * @param keyColumnNumber: index of the column within the passed table that should be indexed
   */
  public RangeEncodedBitmapIndex(AbstractTable table, int keyColumnNumber) {
    super(table, keyColumnNumber);
    this.bitMaps = new TreeMap<T, BitSet>(); // Use TreeMap to get an ordered map impl.
    TableScan tableScan = new TableScan(this.getTable());
    this.bulkLoad(tableScan);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void bulkLoad(TableScanBase tableScan) {
    // TODO Implement this method
    bitmapSize = table.getRecordCount();
    tableScan.open();
    int m = 0;
    do {
      BitSet b = new BitSet(bitmapSize);
      AbstractRecord r = tableScan.next();
      T key = (T) r.getValue(keyColumnNumber);
      if (bitMaps.containsKey(key)) b = bitMaps.get(key);
      for (T key_c : bitMaps.keySet()) {
        if (key_c.compareTo(key) < 0) {
          BitSet b_c = bitMaps.get(key_c);
          b_c.set(m);
          bitMaps.put(key_c,b_c);
        }
        if (key_c.compareTo(key) > 0) {
          BitSet b_c = bitMaps.get(key_c);
          b.or(b_c);
        }
      }
      b.set(m);
      bitMaps.put(key,b);
      System.out.println(b);
      m++;
    }while (m<bitmapSize);
    tableScan.close();
  }

  @Override
  public Iterator<RecordIdentifier> rangeLookup(T startKey, T endKey) {
    // TODO Implement this method
    // init variables
    ArrayList l = new ArrayList();
    BitSet result = bitMaps.get(startKey);
    int max=Integer.MAX_VALUE;
    BitSet b = new BitSet(bitmapSize);
    for (T key : bitMaps.keySet()){
      int m = key.compareTo(endKey);
      if (m>0 && m<max) {
        max = m;
        b = bitMaps.get(key);
      }
    }
    result.xor(b);
    for (int i=0;i<result.cardinality();i++){
      System.out.println("index " + result.nextSetBit(i));
      l.add(table.getRecordIDFromRowNum(result.nextSetBit(i)));
    }
    Iterator it = l.iterator();
    return it;
  }

  @Override
  public boolean isUniqueIndex() {
    return false;
  }
}
