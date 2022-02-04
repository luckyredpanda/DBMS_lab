package de.tuda.dmdb.access;

import de.tuda.dmdb.catalog.objects.Attribute;
import de.tuda.dmdb.catalog.objects.Index;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.EnumSQLType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

/**
 * Abstract class for a table
 *
 * @author cbinnig
 */
public abstract class AbstractTable {
  // data
  protected AbstractRecord prototype; // prototype for all records in table

  // metadata
  protected int recordCount = 0;
  protected Vector<Attribute> attributes = new Vector<Attribute>();
  protected Vector<Attribute> primaryKeys = new Vector<Attribute>();
  protected HashMap<String, Index> indexes = new HashMap<String, Index>();
  protected List<RecordIdentifier> rowNumMapping =
      new ArrayList<RecordIdentifier>(); // maps a rowID to a record in the table

  /**
   * Creates a table for a given record prototype which defines
   *
   * @param prototype prototype of a record
   */
  public AbstractTable(AbstractRecord prototype) {
    this.prototype = prototype;
  }

  /**
   * Returns record prototype
   *
   * @return Prototype record of table
   */
  public AbstractRecord getPrototype() {
    return prototype;
  }

  /**
   * Sets record prototype
   *
   * @param prototype prototype record
   */
  public void setPrototype(AbstractRecord prototype) {
    this.prototype = prototype;
  }

  /**
   * Sets attribute names for table
   *
   * @param attributes vector of attributes
   */
  public void setAttributes(Vector<Attribute> attributes) {
    this.attributes = attributes;
  }

  /**
   * Get attributes of table
   *
   * @return Attributes of table
   */
  public Vector<Attribute> getAttributes() {
    return attributes;
  }

  /**
   * Returns the column number for a given attribute name (if existing). Otherwise, null is
   * returned.
   *
   * @param name Column name
   * @return the index of the column matching the name or null if not found
   */
  public int getColumnNumber(String name) {
    int nr = -1;
    int i = 0;
    for (Attribute attribute : this.attributes) {
      if (attribute.getName().equals(name)) {
        nr = i;
        break;
      }
      i++;
    }
    return nr;
  }

  /**
   * Returns EnumSQLType for given column number
   *
   * @param colNumber column number
   * @return Type of column
   */
  public EnumSQLType getType(int colNumber) {
    return this.prototype.getValue(colNumber).getType();
  }

  /**
   * Returns the recordIdentifier for the given rowNum
   *
   * @param rowNum The rowNum for which to get the recordIdentifier
   * @return The RecordIdentifier corresponding to the provided rowNum
   */
  public RecordIdentifier getRecordIDFromRowNum(Integer rowNum) {
    return this.rowNumMapping.get(rowNum);
  }

  /**
   * Returns the record for a given rowNum of the Bitmap index
   *
   * @param rowNum RowNum as Integer returned by the index
   * @return The AbstractRecord instance in the index table corresponding to the passed rowNum
   */
  public AbstractRecord getRecordFromRowNum(Integer rowNum) {
    return this.lookup(this.rowNumMapping.get(rowNum));
  }

  /**
   * Return a list of records for a given list of rowNums
   *
   * @param rowNums List of rowNums to lookup the record for
   * @return List of AbstractRecords that map to the provided rowNums (resultList[index] maps to
   *     inputList[index])
   */
  public List<AbstractRecord> getRecordFromRowNum(List<Integer> rowNums) {
    List<AbstractRecord> resultList =
        new ArrayList<AbstractRecord>(); // list to store looked up Records
    for (Integer rowNum : rowNums) {
      resultList.add(this.getRecordFromRowNum(rowNum));
    }
    return resultList;
  }

  /**
   * Get primary key attributes
   *
   * @return Primary key attributes
   */
  public Vector<Attribute> getPrimaryKeys() {
    return primaryKeys;
  }

  /**
   * Set primary key attributes
   *
   * @param primaryKeys the primary keys for this table
   */
  public void setPrimaryKeys(Vector<Attribute> primaryKeys) {
    this.primaryKeys = primaryKeys;
  }

  /**
   * Adds metadata of index
   *
   * @param attribute the attribute this index is for
   * @param index the index for the attribute
   */
  public void addIndex(Attribute attribute, Index index) {
    this.indexes.put(attribute.getName(), index);
  }

  /**
   * Get index metadata for attribute
   *
   * @param attribute the attribute
   * @return index metadata belonging to the attribute
   */
  public Index getIndex(Attribute attribute) {
    return this.indexes.get(attribute.getName());
  }

  /**
   * Returns metadata for primary key (if existing)
   *
   * @return index metadata or null if none
   */
  public Index getPrimaryIndex() {
    if (this.primaryKeys.size() == 0) return null;

    Attribute attribute = this.primaryKeys.get(0);
    if (attribute == null) return null;

    return this.indexes.get(attribute.getName());
  }

  /**
   * Returns primary key attribute (if existing). Otherwise, null is returned.
   *
   * @return Primary key attribute
   */
  public Attribute getPrimaryKey() {
    if (this.primaryKeys.size() == 0) return null;

    return this.primaryKeys.get(0);
  }

  /**
   * Returns page number for the nth Page of this table
   *
   * @param index nth page number of this table
   * @return the nth page number of this table
   */
  public abstract Integer getPageNumber(int index);

  /**
   * Returns number of pages this table has
   *
   * @return number of pages this table has
   */
  public abstract int getNumPages();

  /**
   * Returns number of records in table
   *
   * @return Number of records in table
   */
  public int getRecordCount() {
    return recordCount;
  }

  /**
   * Insert a new record into the table
   *
   * @param record the record to be inserted
   * @return RowIdentifier (pagenumber, slotNumber) of record in table
   */
  public abstract RecordIdentifier insert(AbstractRecord record);

  /**
   * Returns a record for a given page and slot number
   *
   * @param pageNumber the page number of the record
   * @param slotNumber the slot in the page
   * @return Record which was found for page and slot
   */
  public abstract AbstractRecord lookup(int pageNumber, int slotNumber);

  /**
   * Returns a record for a given RID
   *
   * @param rid record identifier
   * @return Record which was found at rid
   */
  public abstract AbstractRecord lookup(RecordIdentifier rid);
}
