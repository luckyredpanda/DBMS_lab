package de.tuda.dmdb.execution.advanced;

import de.tuda.dmdb.TestCase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;
@Disabled("Disabled until you implement advanced tests") 
public class TestAdvancedFragmentReplicateJoinPlan extends TestCase {

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

  protected void testFragmentReplicateJoinPlan(
      int startPort, int numNodes, int customerRecords, int orderRecords)
      throws InterruptedException {

    // create customers and orders table
    // create join result table
    // init data structures for each node
    // partition customers table on non customer key
    // partition orders table on order key
    // partition result table on non order key, since customer relation is
    // replicated the local result will
    // contain all local order entries

    // create multiple peers that execute join
    // start all peers
    // wait for all peers to finish
    // all nodes finished, tell nodes to close
    // check the results
  }

  /**
   * Execute a FragmentReplicateJoin with 2 nodes and to bigger input relations
   *
   * @throws InterruptedException
   */
  @Test
  public void testFragmentReplicateJoinPlanWithMoreData() throws InterruptedException {}

  /**
   * Execute a FragmentReplicateJoin with multiple nodes
   *
   * @throws InterruptedException
   */
  @Test
  public void testFragmentReplicateJoinPlanWithMultiplePeers() throws InterruptedException {}
}
