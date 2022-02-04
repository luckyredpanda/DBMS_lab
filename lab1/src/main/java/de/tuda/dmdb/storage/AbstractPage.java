package de.tuda.dmdb.storage;

import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import java.util.Arrays;
import java.util.Objects;

/**
 * Defines the interface of a page
 *
 * @author cbinnig
 */
public abstract class AbstractPage {
  public static int PAGE_SIZE = 64 * 1024; // Bytes
  public static int HEADER_SIZE = 128; // Bytes

  protected byte[] data = new byte[PAGE_SIZE]; // array for page data
  protected int slotSize = 0; // fixed slot size in bytes
  protected int numRecords = 0; // number of records in page
  protected int offset = 0; // offset to next free slot (from page begin)
  // Note: we store the HEADER at the end! (This simplifies the offset calculation for given slot)
  protected int offsetEnd =
      data.length - HEADER_SIZE; // offset for next variable length value (from page end)
  protected int pageNumber = 0;

  /**
   * Creates a page with a given slot size in bytes
   *
   * @param slotSize The size of a slot in bytes
   */
  public AbstractPage(int slotSize) {
    this.slotSize = slotSize;
  }

  /**
   * Get number of records in page
   *
   * @return The number of records stored in the page
   */
  public int getNumRecords() {
    return numRecords;
  }

  /**
   * Returns page number
   *
   * @return The number of this page
   */
  public int getPageNumber() {
    return pageNumber;
  }

  /**
   * Sets page number
   *
   * @param pageNumber Integer value to which to set the page number to
   */
  public void setPageNumber(int pageNumber) {
    this.pageNumber = pageNumber;
  }

  /**
   * Calculates the number of free bytes in a page
   *
   * @return The number of free bytes in a page
   */
  public int getFreeSpace() {
    return offsetEnd - offset;
  }

  /**
   * Calculates if a given record fits into page
   *
   * @param record The record which should fit into the page
   * @return True if there is enough space to store the record, false otherwise
   */
  public boolean recordFitsIntoPage(AbstractRecord record) {
    int fixedLength = record.getFixedLength();
    int variableLength = record.getVariableLength();

    return (fixedLength + variableLength <= getFreeSpace());
  }

  /**
   * Estimates minimum number of records which fit page
   *
   * @param record A record defining the schema (i.e., number and type of attributes)
   * @return number of records of given schema that would fit into the page
   */
  public static int estimateRecords(AbstractRecord record) {
    int maxRecordSize = 0;
    for (AbstractSQLValue value : record.getValues()) {
      maxRecordSize += value.getMaxLength();
      if (!value.isFixedLength()) maxRecordSize += (2 * SQLInteger.LENGTH);
    }
    return (PAGE_SIZE - HEADER_SIZE) / maxRecordSize;
  }

  /**
   * Returns the content of the page as an array of bytes to be used in the storage layer.
   *
   * @return content serialized as an byte-array
   */
  public abstract byte[] serialize();

  /**
   * Replaces the content of this page with the data in the argument.
   *
   * @param data byte array of serialized data
   */
  public abstract void deserialize(byte[] data);

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AbstractPage)) return false;
    AbstractPage that = (AbstractPage) o;
    return slotSize == that.slotSize
        && numRecords == that.numRecords
        && offset == that.offset
        && offsetEnd == that.offsetEnd
        && pageNumber == that.pageNumber
        && Arrays.equals(data, 0, PAGE_SIZE - HEADER_SIZE, that.data, 0, PAGE_SIZE - HEADER_SIZE);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(slotSize, numRecords, offset, offsetEnd, pageNumber);
    result = 31 * result + Arrays.hashCode(data);
    return result;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("AbstractPage{");
    sb.append("data=");
    if (data == null) sb.append("null");
    else {
      sb.append('[');
      for (int i = 0; i < Math.min(128, data.length); ++i) {
        sb.append(String.format("%02X", data[i]));
        if (i % 8 == 7) sb.append(',');
      }
      sb.append(", ...]");
    }
    sb.append(", slotSize=").append(slotSize);
    sb.append(", numRecords=").append(numRecords);
    sb.append(", offset=").append(offset);
    sb.append(", offsetEnd=").append(offsetEnd);
    sb.append(", pageNumber=").append(pageNumber);
    sb.append('}');
    return sb.toString();
  }

  /**
   * Inserts a record into a page at a given slot number while record could be updated (in place) or
   * inserted (into new slot). What are possible edge/problem cases? Throw an appropriate runtime
   * exception.
   *
   * @param slotNumber The number of the slot where the record should be inserted
   * @param record The record to be inserted
   * @param doShift if true: shift fixed length data to fit in the record in the specified
   *     slotNumber. if false: overwrite the slot with new record, note: shifting or removing any
   *     old variable length data is not required
   */
  public abstract void insert(int slotNumber, AbstractRecord record, boolean doShift);

  /**
   * Inserts a record into a page at the next free slot What are possible edge/problem cases? Throw
   * an appropriate runtime exception.
   *
   * @param record The record to be inserted
   * @return slot number where record was inserted
   */
  public abstract int insert(AbstractRecord record);

  /**
   * Returns the record stored at a given slot number What are possible edge/problem cases? Throw an
   * appropriate runtime exception.
   *
   * @param slotNumber Slot number
   * @param record Returned record (must be filled default SqlValues with corresponding types)
   */
  public abstract void read(int slotNumber, AbstractRecord record);
}
