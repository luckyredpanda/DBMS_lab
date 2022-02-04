package de.tuda.dmdb.access;

import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * Bitmap indexes build bitmaps for distinct values of a column and allow to retrieve the rowIds for
 * a lookup expression What and how bitmaps are created are determined by the subclasses.
 *
 * @author melhindi
 * @param <T> Type of the key index by the index.
 */
public abstract class AbstractBitmapIndex<T extends AbstractSQLValue>
    extends AbstractUniqueIndex<T> {

  protected int keyColumnNumber; // column number of key attribute in table
  protected Map<T, BitSet> bitMaps; // bitmap for each distinct value in the column
  protected int bitmapSize; // size of bitmaps, this varies depending on implementation

  public AbstractBitmapIndex(AbstractTable table, int keyColumnNumber) {
    super(table);
    if (this.getTable().getRecordCount() == 0) {
      System.out.println(
          "WARNING: Initializing a Bitmap index with an empty table does not make sense! This Bitmap index is a static index!");
    }

    this.keyColumnNumber = keyColumnNumber;
  }

  /**
   * Reads the table of the BitmapIndex instance and initializes any required bitmaps (member
   * variable bitMaps) for the index's key column (member variable keyColumnNumber) ) according to a
   * naive bitmap approach.
   */
  protected abstract void bulkLoadIndex();

  /**
   * Returns a list of Abstract Records of the index tabled that fall into the specified range.
   * StartKey and endKey are AbstractSQLValue instances that specify the lower- and upper bound of a
   * range. A record falls into the range if {@code startKey <= recordKey <= endKey}.
   *
   * @param startKey AbstractSQLValue instance that specifies the lower bound (inclusive) of the
   *     range to lookup.
   * @param endKey AbstractSQLValue instance that specifies the upper bound (inclusive) of the range
   *     to lookup.
   * @return List of Abstract Records stored in the indexed table and that fall into the specified
   *     range. Returns an empty list if no records fall into the range.
   */
  public abstract List<AbstractRecord> rangeLookup(T startKey, T endKey);

  @Override
  public boolean insert(AbstractRecord record) {
    System.out.println("This method is not supported by this index class");
    return false;
  }

  @Override
  public AbstractRecord lookup(T key) {
    List<AbstractRecord> resultList = this.rangeLookup(key, key);
    if (resultList.isEmpty()) {
      return null;
    }
    return resultList.get(0);
  }

  @Override
  public void print() {
    // print mapping of rowIDs to records
    System.out.println("Records indexed:");
    ArrayList<String> rowIds = new ArrayList<String>();
    ArrayList<String> recordIds = new ArrayList<String>();
    for (int i = 0; i < this.getTable().getRecordCount(); ++i) {
      rowIds.add(Integer.toString(i));
      recordIds.add(this.getTable().getRecordIDFromRowId(i).toString());
    }
    System.out.println("RowIDs   : " + rowIds.toString());
    System.out.println("RecordIDs: " + recordIds.toString());
    // iterate over bitsets and print bitmap
    for (T key : this.bitMaps.keySet()) {
      System.out.println("Bitmap of distinct value " + key);
      System.out.println(this.bitMaps.get(key).toString());
    }
  }

  public int getKeyColumnNumber() {
    return keyColumnNumber;
  }

  public void setKeyColumnNumber(int keyColumnNumber) {
    this.keyColumnNumber = keyColumnNumber;
  }

  public Map<T, BitSet> getBitMaps() {
    return bitMaps;
  }

  public void setBitMaps(Map<T, BitSet> bitMaps) {
    this.bitMaps = bitMaps;
  }
}
