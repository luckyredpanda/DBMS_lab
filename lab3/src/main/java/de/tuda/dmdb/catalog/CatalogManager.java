package de.tuda.dmdb.catalog;

import de.tuda.dmdb.access.AbstractDynamicIndex;
import de.tuda.dmdb.access.AbstractIndex;
import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.access.exercise.*;
import de.tuda.dmdb.catalog.objects.*;
import de.tuda.dmdb.storage.Record;
import java.util.HashMap;
import java.util.Vector;

@SuppressWarnings("rawtypes")
public class CatalogManager {
  private static int LAST_OID = 0;

  private static HashMap<String, Integer> name2oid = new HashMap<>();
  private static HashMap<Integer, DatabaseObject> oid2obj = new HashMap<>();

  private static HashMap<Integer, AbstractTable> tables = new HashMap<>();

  private static HashMap<Integer, AbstractDynamicIndex> uniqueIndexes = new HashMap<>();

  public static synchronized void clear() {
    name2oid.clear();
    oid2obj.clear();
    tables.clear();
    uniqueIndexes.clear();
    LAST_OID = 0;
  }

  /**
   * Creates a new table and registers it in catalog
   *
   * @param tableDesc The table catalog object to use creating and registering the physical table
   * @param attributes The attributes of the table
   * @param types The data-types of the attributes
   * @param primaryKeys List of attributes used as primary key
   * @return AbstractTable representing the physical table that has been created
   */
  public static AbstractTable createTable(
      Table tableDesc,
      Vector<Attribute> attributes,
      Vector<DataType> types,
      Vector<Attribute> primaryKeys) {
    Record prototype = new Record(attributes.size());
    for (int i = 0; i < attributes.size(); ++i) {
      DataType type = types.get(i);
      prototype.setValue(i, type.getType().getInstance(type.getLength()));
    }

    HeapTable table = new HeapTable(prototype);
    table.setAttributes(attributes);
    table.setPrimaryKeys(primaryKeys);

    Integer oid = createOid(tableDesc.getName());
    tableDesc.setOid(oid);
    tables.put(oid, table);
    oid2obj.put(oid, tableDesc);
    return table;
  }

  /**
   * Returns an existing table (if exists, otherwise null is returned)
   *
   * @param name The name of the table to return
   * @return Table from catalog
   */
  public static AbstractTable getTable(String name) {
    Integer oid = getOid(name);
    return tables.get(oid);
  }

  /**
   * Creates an index and registers it in catalog
   *
   * @param indexDesc The index catalog object describing the physical index to create and register
   * @param tableDesc The table catalog object referring to the table being indexed
   * @param attDesc The attribute on which the index is build
   * @return Unique index which was created
   */
  public static AbstractIndex createUniqueIndex(
      Index indexDesc, Table tableDesc, Attribute attDesc) {
    AbstractTable table = getTable(tableDesc.getName());
    AbstractIndex index = null;

    int key = table.getColumnNumber(attDesc.getName());
    switch (indexDesc.getType()) {
      case BPlusTree:
        index = new UniqueBPlusTree(table, key);
        break;
      case NaiveBitmap:
        index = new NaiveBitmapIndex(table, key);
        break;
      case ApproximateBitmap:
        index = new ApproximateBitmapIndex(table, key);
        break;
      case RangeEncodedBitmap:
        index = new RangeEncodedBitmapIndex(table, key);
        break;
    }

    Integer oid = createOid(indexDesc.getName());
    indexDesc.setOid(oid);
    if (index.isUniqueIndex()) {
      uniqueIndexes.put(oid, (AbstractDynamicIndex) index);
    }
    oid2obj.put(oid, indexDesc);

    // register index at table
    table.addIndex(attDesc, indexDesc);

    return index;
  }

  /**
   * Returns an existing index (if exists, otherwise null is returned)
   *
   * @param name The name of the index to retrieve
   * @return Unique index form catalog
   */
  public static AbstractDynamicIndex getUniqueIndex(String name) {
    Integer oid = getOid(name);
    return uniqueIndexes.get(oid);
  }

  /**
   * Returns oid for table or index
   *
   * @param name The name of the object to retrieve
   * @return Oid as integer for the object with the passed name
   */
  public static Integer getOid(String name) {
    if (name2oid.containsKey(name)) {
      return name2oid.get(name);
    }
    return 0;
  }

  /**
   * Returns database object for oid
   *
   * @param oid ObjectId of the database object to retrieve
   * @return The database object with the passed id
   */
  public static DatabaseObject getDatabaseObject(Integer oid) {
    return oid2obj.get(oid);
  }

  /**
   * Creates or reuses oid for table or index
   *
   * @param name The name of the catalog object to create an oid for
   * @return the object id to use for the catalog object
   */
  public static Integer createOid(String name) {
    if (name2oid.containsKey(name)) {
      return name2oid.get(name);
    }
    int oid = ++LAST_OID;
    name2oid.put(name, oid);
    return oid;
  }
}
