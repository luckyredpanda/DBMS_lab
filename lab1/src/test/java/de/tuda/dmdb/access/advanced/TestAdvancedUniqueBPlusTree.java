package de.tuda.dmdb.access.advanced;

import de.tuda.dmdb.TestCase;
import org.junit.jupiter.api.Test;

public class TestAdvancedUniqueBPlusTree extends TestCase {

  /** Insert three records and lookup not existing keys */
  @Test
  public void testIndexKeyNotExisting() {}

  /** Insert many records with increasing keys and do lookup for each key afterwards */
  @Test
  public void testIndexForwardInsert() {}

  /** Insert many records with decreasing keys and do lookup for each key afterwards */
  @Test
  public void testIndexReverseInsert() {}
}
