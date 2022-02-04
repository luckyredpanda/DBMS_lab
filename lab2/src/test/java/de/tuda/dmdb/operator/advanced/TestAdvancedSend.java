package de.tuda.dmdb.operator.advanced;

import de.tuda.dmdb.TestCase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;
@Disabled("Disabled until you implement advanced tests") 
public class TestAdvancedSend extends TestCase {

  /**
   * Test that Send Operator sends data over the socket
   *
   * @throws InterruptedException when sleep after server start is interrupted
   */
  @Test
  public void testDataIsSentSimple() throws InterruptedException {
    // initialize

    // set up server that should receive the remote tuples
    // create tcp server to listen for in-coming connections

    // init the send operator and start processing
  }

  /**
   * Test that multiple records are sent to multiple peers
   *
   * @throws InterruptedException when sleep after server start is interrupted
   */
  @Test
  public void testDataIsSentComplex() throws InterruptedException {
    // initialize

    // distribution function that sends to all peers

    // create tcp server to listen for in-coming connections

    // init the send operator and start processing

    // Each server should have received the numRecords records
  }
}
