package de.tuda.dmdb.access.advanced;

import de.tuda.dmdb.TestCase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;
@Disabled("Disabled until you implement advanced tests") 
public class TestAdvancedRangeEncodedBitmapIndex extends TestCase {

  /** Insert records with same value should create only required bitmaps */
  @Test
  public void testBulkLoadNaive() {}

  /** Insert multiple records using a SQLInteger index and test that all retrieved */
  @Test
  public void testRangeLookupAll() {
    // insert

    // lookup

    // lookup should return all records
  }

  /** Insert multiple records using a SQLInteger index and test that matching records retrieved */
  @Test
  public void testRangeLookupCondition() {
    // insert

    // lookup

    // lookup should return all records
  }

  /** Insert multiple records using a SQLInteger index and test that matching records retrieved */
  @Test
  public void testRangeLookupNaive() {}

  /** Insert many records with increasing keys and do lookup for each key afterwards */
  @Test
  public void testIndexForwardInsert() {
    // insert

    // lookup
  }

  /** Insert many records with decreasing keys and do lookup for each key afterwards */
  @Test
  public void testIndexReverseInsert() {
    // insert

    // lookup
  }
}
