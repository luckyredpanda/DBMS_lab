package de.tuda.dmdb.testhelper;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/** Using this JUnit5 extensions for printing the time each test took. */
public class TimingExtension implements BeforeEachCallback, AfterEachCallback {

  long startMillis;

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    long end = System.currentTimeMillis();

    Logger.getLogger("timing")
        .log(
            Level.INFO,
            "Execution of " + context.getDisplayName() + " took " + (end - startMillis) + "ms.");
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    startMillis = System.currentTimeMillis();
  }
}
