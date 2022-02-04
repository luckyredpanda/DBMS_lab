package de.tuda.dmdb.operator.advanced;

import de.tuda.dmdb.TestCase;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;
@Disabled("Disabled until you implement advanced tests") 
public class TestAdvancedReplicationExchange extends TestCase {

  /**
   * Test ReplicationExchange with multiple peers
   *
   * @throws InterruptedException if threads are unable to be joined
   */
  @Test
  public void testExchangeWithMultiplePeers() throws InterruptedException {

    // create a table for each peer and populate nodeMap

    // create multiple peers that exchange data

    // test that all nodes received all records
  }

  /**
   * Test ReplicationExchange with many peers sending a different number of records Simulates that
   * some peers might be faster than others
   *
   * @throws InterruptedException
   */
  @Test
  public void testPeersSendDifferentNumberOfRecords() throws InterruptedException {
    // create a table for each peer with different number of records and
    // populate nodeMap

    // create multiple peers that exchange data
    // start all peers
    // wait for all peers to finish
    // test that all nodes received all records
  }

  /**
   * Execute testPeersSendDifferentNumberOfRecords multiple times
   *
   * @throws InterruptedException
   */
  @Test
  public void penTest() throws InterruptedException {}
}
