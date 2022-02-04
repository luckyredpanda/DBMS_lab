package de.tuda.dmdb.operator.advanced;

import de.tuda.dmdb.TestCase;
import java.io.IOException;
import java.net.UnknownHostException;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.Disabled;
@Disabled("Disabled until you implement advanced tests") 
public class TestAdvancedReceive extends TestCase {

  /** Test that local records are processed even if there are no connections */
  @Test
  public void testReceiveNoPeerProcessesLocalRecords() {
    // create a receiveOperator that process records from a table scan

    // process local records by calling next on the receive operator
  }

  /**
   * Test that the receiveOperator receives data from one peer
   *
   * @throws IOException
   * @throws UnknownHostException
   */
  @Test
  public void testReceiveOnePeer() throws UnknownHostException, IOException {
    // create a receiveOperator that process records from a table scan and expects a
    // second peer

    // process the local and remote records

    // expect correct transmission
  }

  /**
   * Test that the receiveOperator receives data from multiple peers and thus handles
   * multi-threading
   *
   * @throws InterruptedException
   */
  @Test
  public void testReceiveMultiplePeers() throws InterruptedException {
    // create a table for each peer
    // populate each peers table

    // create multiple clients that will send data to the receive operator

    // initialize the server, so that the clients can connect

    // let receive operator process all the records

    // wait until all clients finished sending
  }

  /**
   * Run testReceiveMultiplePeers multiple times
   *
   * @throws InterruptedException
   */
  @Test
  public void testReceiveMultiplePeersPenTest() throws InterruptedException {}
}
