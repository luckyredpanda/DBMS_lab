package de.tuda.dmdb.operator.advanced;

import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.Disabled;
@Disabled("Disabled until you implement advanced tests") 
public class TestAdvancedIndexScan {

  /**
   * Tests that a NullPointerException is thrown if next() is called before open() on IndexScan
   * operator
   */
  @Test
  public void testIndexScanInit() {}

  /** Tests that IndexScan returns records with only the selected attributes */
  @Test
  public void testIndexScanReturnsOnlyCorrectRecord() {}

  /** Tests that IndexScan returns no entries when none in range */
  @Test
  public void testIndexScanNotInRange() {}

  /** Tests that IndexScan returns correct records at an high value */
  @Test
  public void testIndexScanHighValue() {}
}
