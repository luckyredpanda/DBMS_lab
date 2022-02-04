package de.tuda.dmdb;

import org.junit.Rule;
import org.junit.rules.Timeout;

public abstract class TestCase {

  @Rule public Timeout globalTimeout = Timeout.seconds(45); // 45 seconds max per method tested

  /*@Before
  public void setUp() {
    CatalogManager.clear();
  } */
}
