package de.tuda.dmdb.testhelper;

import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.withSettings;

import de.tuda.dmdb.buffer.DummyBufferManager;
import de.tuda.dmdb.buffer.exercise.BufferManager;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.MockedStatic;

/** Using this JUnit5 extensions a test can use the DummyBufferManager. */
public class BufferManagerMocking implements BeforeEachCallback, AfterEachCallback {

  DummyBufferManager dummyBufferManager = new DummyBufferManager();
  MockedStatic<BufferManager> mockedStatic;

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    this.mockedStatic.close();
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    this.mockedStatic = mockStatic(BufferManager.class, withSettings().stubOnly());
    mockedStatic.when(BufferManager::getInstance).thenReturn(dummyBufferManager);
  }
}
