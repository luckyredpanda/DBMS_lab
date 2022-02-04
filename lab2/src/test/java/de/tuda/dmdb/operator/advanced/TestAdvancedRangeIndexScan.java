package de.tuda.dmdb.operator.advanced;

import de.tuda.dmdb.TestCase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;
@Disabled("Disabled until you implement advanced tests") 
public class TestAdvancedRangeIndexScan extends TestCase {

  /**
   * Tests that a NullPointerException is thrown if next() is called before open() on IndexScan
   * operator
   */
  @Test
  public void testRangeScanInit() {}

  /** Tests that RangeScan returns correct records for range */
  @Test
  public void testRangeScanNextMultiple() {}

  /** Tests that RangeScan returns no entries when none in range */
  @Test
  public void testRangeScanNotInRange() {}

  /** Tests that RangeScan returns correct records for range */
  @Test
  public void testRangeScanHighRange() {}

  /** Tests that RangeScan returns all for full range */
  @Test
  public void testRangeScanFullRange() {}
}
