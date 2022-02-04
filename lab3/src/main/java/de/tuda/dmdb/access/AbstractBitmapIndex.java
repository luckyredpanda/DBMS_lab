package de.tuda.dmdb.access;

import de.tuda.dmdb.storage.types.AbstractSQLValue;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

/**
 * Bitmap indexes build bitmaps for distinct values of a column and allow to retrieve the row-number
 * (rowNum) for a lookup expression. What and how bitmaps are created are determined by the
 * subclasses.
 *
 * @author melhindi
 * @param <T> Type of the key index by the index.
 */
public abstract class AbstractBitmapIndex<T extends AbstractSQLValue> extends AbstractIndex<T> {

  protected int keyColumnNumber; // column number of key attribute in table
  /**
   * Note that we only store bitMaps in memory, i.e., we do not store bitmaps in pages and use the
   * BufferManager to persist them automatically. This will be added in future.
   */
  protected Map<T, BitSet> bitMaps; // bitmap for each distinct value in the column

  protected int bitmapSize; // size of bitmaps, this varies depending on implementation

  protected AbstractBitmapIndex(AbstractTable table, int keyColumnNumber) {
    super(table);
    if (this.getTable().getRecordCount() == 0) {
      System.out.println(
          "WARNING: Initializing a Bitmap index with an empty table does not make sense! This Bitmap index is a static index!");
    }

    this.keyColumnNumber = keyColumnNumber;
  }

  @Override
  public Iterator<RecordIdentifier> lookup(T key) {
    return this.rangeLookup(key, key);
  }

  @Override
  public void print() {
    // print mapping of rowIDs to records
    System.out.println("Records indexed:");
    ArrayList<String> rowIds = new ArrayList<>();
    ArrayList<String> recordIds = new ArrayList<>();
    for (int i = 0; i < this.getTable().getRecordCount(); ++i) {
      rowIds.add(Integer.toString(i));
      recordIds.add(this.getTable().getRecordIDFromRowNum(i).toString());
    }
    System.out.println("RowIDs   : " + rowIds.toString());
    System.out.println("RecordIDs: " + recordIds.toString());
    // iterate over bit-sets and print bitmap
    for (Entry<T, BitSet> entry : this.bitMaps.entrySet()) {
      System.out.println("Bitmap of distinct value " + entry.getKey());
      System.out.println(entry.getValue().toString());
    }
  }

  /**
   * Return the number of the column that is being indexed
   *
   * @return Column number as int
   */
  public int getKeyColumnNumber() {
    return keyColumnNumber;
  }

  /**
   * Return the HashMap that we use to track the bitmaps per key
   *
   * @return HashMap containing a bit-set per key
   */
  public Map<T, BitSet> getBitMaps() {
    return bitMaps;
  }

  /**
   * Set the hashMap containing the bit-sets per key
   *
   * @param bitMaps Hashmap containing a BitSet per key
   */
  public void setBitMaps(Map<T, BitSet> bitMaps) {
    this.bitMaps = bitMaps;
  }

  /**
   * Iterator class that provides access to RecordIdentifiers of records matching a certain index
   * query
   */
  protected class BitmapIterator implements Iterator<RecordIdentifier> {
    // State for this iterator
    BitSet resultBitMap; // bitset of qualifying records
    int cursor; // cursor to store the next record to return by the iterator

    /**
     * Constructor that initializes the iterator
     *
     * @param resultBitMap Bitset of qualifying records
     */
    public BitmapIterator(BitSet resultBitMap) {
      this.resultBitMap = resultBitMap;
      this.cursor = resultBitMap.nextSetBit(0);
    }

    /**
     * Indicate if iterator provides more records
     *
     * @return True if next() call returns a next element, false if there are no additional elements
     */
    public boolean hasNext() {
      return (this.cursor >= 0);
    }

    /**
     * Return current data and update cursor
     *
     * @return the next RecordIdentifier matching the query
     */
    public RecordIdentifier next() {
      // as per Iterator<> contract we have to throw an exception if there are no more records
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      RecordIdentifier identifier = getTable().getRecordIDFromRowNum(cursor);
      // update cursor
      if (this.cursor == Integer.MAX_VALUE) {
        this.cursor = -1; // or (cursor+1) would overflow
      } else {
        this.cursor = resultBitMap.nextSetBit(cursor + 1);
      }

      return identifier;
    }
  }
}
