package de.tuda.dmdb;

import de.tuda.dmdb.buffer.exercise.BufferManager;
import de.tuda.dmdb.catalog.CatalogManager;
import de.tuda.dmdb.storage.exercise.DiskManager;
import java.io.File;
import org.junit.jupiter.api.BeforeEach;

public abstract class TestCase {

  @BeforeEach
  public void cleanup() {
    File f = new File(DiskManager.DB_FILENAME);
    if (f.exists()) f.delete();
    BufferManager.clearInstance();
    CatalogManager.clear();
  }
}
