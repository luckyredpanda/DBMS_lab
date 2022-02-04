package de.tuda.dmdb;

import de.tuda.dmdb.buffer.exercise.BufferManager;
import de.tuda.dmdb.storage.exercise.DiskManager;
import de.tuda.dmdb.testhelper.TimingExtension;
import java.io.File;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TimingExtension.class)
public abstract class TestCase {

  @BeforeEach
  public void cleanup() {
    File f = new File(DiskManager.DB_FILENAME);
    if (f.exists()) f.delete();
    BufferManager.clearInstance();
  }
}
