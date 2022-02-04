package de.tuda.dmdb.operator.advanced;

import de.tuda.dmdb.TestCase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;



/**
 * Test for the GroupByAggregate operator
 *
 * @author melhindi
 */
@Disabled("Disabled until you implement advanced tests")  public class TestAdvancedGroupByAggregate extends TestCase {

  /** Test that open initializes the operator */
  @Test
  public void testGroupByInit() {
    // call next on operator without initializing it should throw an exception
  }

  /** Test group by multiple SQLInteger columns and count */
  @Test
  public void testGroupByMultipleSQLIntegerColumns() {}

  /** Test group by multiple SQLVarchar columns and count (aggregate attribute is a SQLInteger) */
  @Test
  public void testGroupByMultipleSQLVarcharColumns() {}

  /** Test group by on second column and min aggregate on the first column */
  @Test
  public void testAggregationMin() {}

  /** Test no group by and min aggregate on the first column */
  @Test
  public void testAggregationMinNoGroup() {}

  /** Test aggregating multiple columns with different aggregation methods. */
  @Test
  public void testAggregationMultipleColumns() {}
}
