package de.tuda.dmdb.catalog;

import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.access.AbstractUniqueIndex;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.access.exercise.UniqueBPlusTree;
import de.tuda.dmdb.catalog.objects.Attribute;
import de.tuda.dmdb.catalog.objects.DataType;
import de.tuda.dmdb.catalog.objects.DatabaseObject;
import de.tuda.dmdb.catalog.objects.Index;
import de.tuda.dmdb.catalog.objects.Table;
import de.tuda.dmdb.storage.Record;
import java.util.HashMap;
import java.util.Vector;

@SuppressWarnings("rawtypes")
public class CatalogManager {
  private static int LAST_OID = 0;

  private static HashMap<String, Integer> name2oid = new HashMap<String, Integer>();
  private static HashMap<Integer, DatabaseObject> oid2obj = new HashMap<Integer, DatabaseObject>();

  private static HashMap<Integer, AbstractTable> tables = new HashMap<Integer, AbstractTable>();

  private static HashMap<Integer, AbstractUniqueIndex> uniqueIndexes =
      new HashMap<Integer, AbstractUniqueIndex>();

  public static synchronized void clear() {
    name2oid.clear();
    oid2obj.clear();
    tables.clear();
    uniqueIndexes.clear();
    LAST_OID = 0;
  }

  /**
   * creates a new table and registers it in catalog
   *
   * @param name table name
   * @param attributes list of attributes
   * @param types list of data types
   * @return Table which was created
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
   * @param name
   * @return Table from catalog
   */
  public static AbstractTable getTable(String name) {
    Integer oid = getOid(name);
    return tables.get(oid);
  }

  /**
   * Creates an index an registers it in catalog
   *
   * @param indexDesc
   * @param tableDesc
   * @param attDesc
   * @return Unique index which was created
   */
  public static AbstractUniqueIndex createUniqueIndex(
      Index indexDesc, Table tableDesc, Attribute attDesc) {
    AbstractTable table = getTable(tableDesc.getName());
    AbstractUniqueIndex index = null;

    int key = table.getColumnNumber(attDesc.getName());
    switch (indexDesc.getType()) {
      case BPlusTree:
        index = new UniqueBPlusTree(table, key);
        break;
    }

    Integer oid = createOid(indexDesc.getName());
    indexDesc.setOid(oid);
    uniqueIndexes.put(oid, index);
    oid2obj.put(oid, indexDesc);

    // register index at table
    table.addIndex(attDesc, indexDesc);

    return index;
  }

  /**
   * Returns an existing index (if exists, otherwise null is returned)
   *
   * @param name
   * @return Unique index form catalog
   */
  public static AbstractUniqueIndex getUniqueIndex(String name) {
    Integer oid = getOid(name);
    return uniqueIndexes.get(oid);
  }

  /**
   * Returns oid for table or index
   *
   * @param name
   * @return
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
   * @param oid
   * @return
   */
  public static DatabaseObject getDatabaseObject(Integer oid) {
    return oid2obj.get(oid);
  }

  /**
   * Creates or reuses oid for table or index
   *
   * @param name
   * @return
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
