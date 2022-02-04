package de.tuda.dmdb.storage;

import de.tuda.dmdb.storage.exercise.DiskManager;
import de.tuda.dmdb.storage.exercise.RowPage;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;
import java.io.File;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestDiskManager {

  // remove any existing DB file before use
  @BeforeEach
  public void removeExistingDBFile() {
    File f = new File(DiskManager.DB_FILENAME);
    if (f.exists()) f.delete();
  }

  /** Test that a page can be written to disk */
  @Test
  public void testInsertOnePage() {
    DiskManager diskManager = new DiskManager();

    // insert record
    AbstractRecord r1 = new Record(2);
    r1.setValue(0, new SQLInteger(0x12345678));
    r1.setValue(1, new SQLVarchar("Test", 10));
    AbstractPage p = new RowPage(r1.getFixedLength());
    p.insert(r1);

    diskManager.writePage(p);
    File f = new File(DiskManager.DB_FILENAME);

    // assert content is actually written
    Assertions.assertTrue(f.exists());
    AbstractPage pcmp = diskManager.readPage(0);

    // compare
    Assertions.assertEquals(p, pcmp, "The read page should equal the previously inserted one.");
    diskManager.close();
  }

  /** Simple test to check if multiple pages can be written to disk */
  @Test
  public void testInsertMultiplePage() {
    DiskManager diskManager = new DiskManager();

    // insert record
    AbstractRecord r1 = new Record(2);
    r1.setValue(0, new SQLInteger(123456789));
    r1.setValue(1, new SQLVarchar("Test", 10));
    AbstractPage p1 = new RowPage(r1.getFixedLength());
    p1.insert(r1);
    p1.setPageNumber(0);
    AbstractPage p2 = new RowPage(r1.getFixedLength());
    p2.insert(r1);
    p2.setPageNumber(1);
    AbstractPage p3 = new RowPage(r1.getFixedLength());
    p3.insert(r1);
    p3.setPageNumber(2);
    AbstractPage p4 = new RowPage(r1.getFixedLength());
    p4.insert(r1);
    p4.setPageNumber(3);

    diskManager.writePage(p1);
    diskManager.writePage(p2);
    diskManager.writePage(p3);
    diskManager.writePage(p4);
    File f = new File(DiskManager.DB_FILENAME);

    // assert content is actually written
    Assertions.assertTrue(f.exists() && f.length() >= 4 * RowPage.PAGE_SIZE);

    Assertions.assertEquals(
        p1, diskManager.readPage(0), "The read pages should equal the previously inserted one.");
    Assertions.assertEquals(
        p2, diskManager.readPage(1), "The read pages should equal the previously inserted one.");
    Assertions.assertEquals(
        p3, diskManager.readPage(2), "The read pages should equal the previously inserted one.");
    Assertions.assertEquals(
        p4, diskManager.readPage(3), "The read pages should equal the previously inserted one.");

    diskManager.close();
  }

  /** Test invalid write */
  @Test
  public void testInvalidWrite() {
    DiskManager diskManager = new DiskManager();
    try {
      diskManager.writePage(null);
      Assertions.fail("DiskManager should not be able to write invalid pages.");
    } catch (Exception ignored) {
    }
    diskManager.close();
  }

  /** Test invalid read */
  @Test
  public void testInvalidRead() {
    DiskManager diskManager = new DiskManager();

    try {
      AbstractPage p = diskManager.readPage(-1);
      Assertions.assertNull(p);
    } catch (Exception ex) {
      // also fine. Either throw exception or return null
    }
    diskManager.close();
  }
}
