package de.tuda.dmdb.operator.advanced;

import de.tuda.dmdb.TestCase;
import org.junit.jupiter.api.Test;

/** Test for the Rollup operator */
import org.junit.jupiter.api.Disabled;
@Disabled("Disabled until you implement advanced tests") 
public class TestAdvancedRollup extends TestCase {

  /** Test that open initializes the operator */
  @Test
  public void testRollupInit() {
    // call next on operator without initializing it should throw an exception
  }

  /** Test group by multiple SQLInteger columns and count */
  @Test
  public void testGroupByMultipleSQLIntegerColumns() {
    // initialize
    // test that all expected elements are in the resultlist
  }

  /** Test group by multiple SQLVarchar columns and count */
  @Test
  public void testGroupByMultipleSQLVarcharColumns() {
    // initialize
    // test that all expected elements are in the resultlist
  }

  /** Test group by multiple SQLInteger columns and count mid column */
  @Test
  public void testGroupByMultipleSQLIntegerColumnsAggregateMidColumn() {
    // initialize
    // test that all expected elements are in the resultlist
  }

  /** Test group by on first columns and min aggregate on the last column */
  @Test
  public void testAggregationMin() {
    // initialize
    // test that all expected elements are in the resultlist
  }

  /** Test group by on first columns and min aggregate on the last column */
  @Test
  public void testAggregationMultipleAggregatesOnSameColumn() {
    // initialize
    // test that all expected elements are in the resultlist
  }
}
