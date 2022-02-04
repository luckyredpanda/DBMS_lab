package de.tuda.dmdb.execution.advanced;

import de.tuda.dmdb.TestCase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;
@Disabled("Disabled until you implement advanced tests") 
public class TestAdvancedSemiJoinReductionPlan extends TestCase {

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

  protected void testSemiJoinReductionPlan(
      int startPort, int numNodes, int customerRecords, int orderRecords)
      throws InterruptedException {

    // create customers and orders table
    // create semi-join result table contains all customers that have an order
    // init data structures for each node
    // partition customers table on customer key
    // partition orders table on order key
    // partition result table on customer key

    // create multiple peers that execute join
    // start all peers
    // wait for all peers to finish
    // all nodes finished, tell nodes to close
    // check the results
  }

  /**
   * Execute a SemiJoinReduction with 2 nodes and to bigger input relations
   *
   * @throws InterruptedException
   */
  @Test
  public void testSemiJoinReductionPlanWithMoreData() throws InterruptedException {}

  /**
   * Execute a SemiJoinReductionJoin with multiple nodes
   *
   * @throws InterruptedException
   */
  @Test
  public void testSemiJoinReductionJoinPlanWithMultiplePeers() throws InterruptedException {}
}
