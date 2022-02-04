package de.tuda.dmdb.operator.advanced;

import de.tuda.dmdb.TestCase;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;

/**
 * Test that HashRepartitionExchange works correctly for different numbers of peer nodes
 *
 * @author melhindi
 */
@Disabled("Disabled until you implement advanced tests") 
public class TestAdvancedHashRepartitionExchangeNumberOfPeers extends TestCase {

  /** Test that HashRepartitionExchange keeps all data local if there is no peer */
  @Test
  public void testShuffleNoPeer() {}

  /**
   * Create two Exchange operators and test that they shuffle data correctly
   *
   * @throws InterruptedException
   */
  @Test
  public void testShuffleOnePeer() throws InterruptedException {}

  /**
   * Create multiple Exchange operators and test that they shuffle data around correctly
   *
   * @throws InterruptedException
   */
  @Test
  public void testShuffleMultiplePeers() throws InterruptedException {}

  /**
   * Test that two nodes shuffle data around and produce join result correctly
   *
   * @throws InterruptedException
   */
  @Test
  public void testShuffleOnePeerJoin() throws InterruptedException {}
}
