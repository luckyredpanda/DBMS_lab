package de.tuda.dmdb.execution.advanced;

import de.tuda.dmdb.TestCase;
import java.util.Vector;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.Disabled;
@Disabled("Disabled until you implement advanced tests") 
public class TestAdvancedDistributedCountPlan extends TestCase {

  /**
   * Test that the operator plan tree contains the expected operators
   *
   * @throws InterruptedException
   */
  @Test
  public void testCorrectOperatorPlanTree() throws InterruptedException {
    // init distributed plan

    // walk operator tree from root to leaf and check operator type
  }

  protected void testDistributedCountPlan(
      int startPort,
      int numNodes,
      int customerRecords,
      int numGroups,
      Vector<Integer> groupByAttributes)
      throws InterruptedException {
    // create customers table
    // init data structures for each node
    // partition customers table on customer key

    // create expected result by computing it locally

    // create multiple peers that execute count locally and sent their result to the
    // coordinator
    // start all peers
    // wait for all peers to finish
    // all nodes finished, tell nodes to close
    // check that only coordinator has a result
  }

  /**
   * Execute distributed count with multiple nodes and no grouping
   *
   * @throws InterruptedException
   */
  @Test
  public void testDistributedCountPlanWithoutGroupBy() throws InterruptedException {}

  /**
   * Execute distributed count with multiple nodes and grouping by one column
   *
   * @throws InterruptedException
   */
  @Test
  public void testDistributedCountPlanWithSingleColumnGroupBy() throws InterruptedException {}

  /**
   * Execute distributed count with multiple nodes and grouping by two columns
   *
   * @throws InterruptedException
   */
  @Test
  public void testDistributedCountPlanWithTwoColumnsGroupBy() throws InterruptedException {}
}
