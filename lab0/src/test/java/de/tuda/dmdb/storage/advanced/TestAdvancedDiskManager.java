
package de.tuda.dmdb.storage.advanced;

import de.tuda.dmdb.storage.AbstractPage;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.exercise.DiskManager;
import de.tuda.dmdb.storage.exercise.RowPage;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;
import java.io.File;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestAdvancedDiskManager {
  @BeforeEach
  public void removeExistingDBFile() {
    File f = new File("testdb.sdms");
    if (f.exists())
      f.delete(); 
  }
  
  public AbstractPage generatePage(int pageNum) {
    int numRec = (int)Math.round(Math.random() * 10.0D + 10.0D);
    AbstractRecord r1 = generateRecord();
    RowPage rowPage = new RowPage(r1.getFixedLength());
    while (--numRec > 0)
      rowPage.insert(generateRecord()); 
    rowPage.setPageNumber(pageNum);
    return (AbstractPage)rowPage;
  }
  
  public AbstractRecord generateRecord() {
    int rand = (int)Math.round(Math.random() * 100.0D);
    Record record = new Record(2);
    record.setValue(0, (AbstractSQLValue)new SQLInteger(34887555 * rand));
    record.setValue(1, (AbstractSQLValue)new SQLVarchar("Test" + rand, 20));
    return (AbstractRecord)record;
  }
  
  @Test
  public void testInsertOnePageRandom() {
    DiskManager diskManager = new DiskManager();
    int PAGE_NUM = 12;
    AbstractPage p = generatePage(PAGE_NUM);
    diskManager.writePage(p);
    AbstractPage pcmp = diskManager.readPage(Integer.valueOf(PAGE_NUM));
    File f = new File("testdb.sdms");
    Assertions.assertTrue((f.exists() && f.length() >= (PAGE_NUM * RowPage.PAGE_SIZE)));
    Assertions.assertEquals(p, pcmp, "The read page should equal the previously inserted one.");
    diskManager.close();
  }
  
  @Test
  public void testPageInPersistedStorage() {
    int PAGE_NUM = 46;
    AbstractPage p = generatePage(PAGE_NUM);
    AbstractPage pcmp = null;
    DiskManager diskManager = new DiskManager();
    diskManager.writePage(p);
    diskManager.close();
    DiskManager diskManager2 = new DiskManager();
    pcmp = diskManager2.readPage(Integer.valueOf(PAGE_NUM));
    diskManager2.close();
    Assertions.assertEquals(p, pcmp, "The read page should equal the previously inserted one.");
  }
  
  @Test
  @Timeout(1L)
  public void testLargeDB() {
    int PAGE_NUM = 1048576 / RowPage.PAGE_SIZE;
    AbstractPage p = generatePage(PAGE_NUM);
    AbstractPage pcmp = null;
    DiskManager diskManager = new DiskManager();
    diskManager.writePage(p);
    diskManager.close();
    DiskManager diskManager2 = new DiskManager();
    pcmp = diskManager2.readPage(Integer.valueOf(PAGE_NUM));
    diskManager2.close();
    Assertions.assertEquals(p, pcmp, "The read page should equal the previously inserted one.");
  }
  
  @Test
  public void testInsertMultiplePageRandomOrder() {
    DiskManager diskManager = new DiskManager();
    AbstractPage p1 = generatePage(0);
    AbstractPage p2 = generatePage(1);
    AbstractPage p3 = generatePage(2);
    AbstractPage p4 = generatePage(3);
    AbstractPage p10 = generatePage(9);
    diskManager.writePage(p10);
    diskManager.writePage(p2);
    diskManager.writePage(p3);
    diskManager.writePage(p1);
    diskManager.writePage(p4);
    File f = new File("testdb.sdms");
    Assertions.assertTrue((f.exists() && f.length() >= (4 * RowPage.PAGE_SIZE)));
    Assertions.assertEquals(p1, diskManager
        .readPage(Integer.valueOf(0)), "The read pages should equal the previously inserted one.");
    Assertions.assertEquals(p2, diskManager
        .readPage(Integer.valueOf(1)), "The read pages should equal the previously inserted one.");
    Assertions.assertEquals(p3, diskManager
        .readPage(Integer.valueOf(2)), "The read pages should equal the previously inserted one.");
    Assertions.assertEquals(p4, diskManager
        .readPage(Integer.valueOf(3)), "The read pages should equal the previously inserted one.");
    Assertions.assertEquals(p10, diskManager
        .readPage(Integer.valueOf(9)), "The read pages should equal the previously inserted one.");
    diskManager.close();
  }
  
  @Test
  public void testAdvancedInvalidWrite() {
    DiskManager diskManager = new DiskManager();
    RowPage rowPage = new RowPage(10);
    rowPage.setPageNumber(-10);
    try {
      diskManager.writePage((AbstractPage)rowPage);
      Assertions.fail("DiskManager should not be able to write invalid pages.");
    } catch (Exception exception) {}
    diskManager.close();
  }
  
  @Test
  public void testAdvancedInvalidRead() {
    File f = new File("testdb.sdms");
    if (f.exists())
      throw new RuntimeException("Test could not be executed successfully"); 
    DiskManager diskManager = new DiskManager();
    try {
      AbstractPage p = diskManager.readPage(Integer.valueOf(123456));
      Assertions.assertNull(p);
    } catch (Exception exception) {}
    diskManager.close();
  }
}
