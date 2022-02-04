package de.tuda.dmdb.operator.advanced;

import de.tuda.dmdb.TestCase;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;

@Disabled("Disabled until you implement advanced tests") public class TestAdvancedGatherExchange extends TestCase {

  /**
   * Test GatherExchange with multiple peers
   *
   * @throws InterruptedException
   */
  @Test
  public void testExchangeWithMultiplePeers() throws InterruptedException {
    // create a table for each peer and populate nodeMap
    // populate each peers table

    // create multiple peers that exchange data

    // start all peers

    // wait for all peers to finish

    // test that coordinator received all records
  }

  /**
   * Test GatherExchange with many peers sending a different number of records Simulates that some
   * peers might be faster than others
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
    // test that coordinator received all records
  }

  /**
   * Execute testPeersSendDifferentNumberOfRecords multiple times
   *
   * @throws InterruptedException
   */
  @Test
  public void penTest() throws InterruptedException {}
}
