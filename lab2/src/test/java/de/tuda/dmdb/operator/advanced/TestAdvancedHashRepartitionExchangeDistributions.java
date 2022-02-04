package de.tuda.dmdb.operator.advanced;

import de.tuda.dmdb.TestCase;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;


/**
 * Test that HashRepartitionExchange is able to deal correctly with different distributions
 *
 * @author melhindi
 */
@Disabled("Disabled until you implement advanced tests") 
public class TestAdvancedHashRepartitionExchangeDistributions extends TestCase {

  /**
   * Create two Exchange operators and test that they correctly deal with skewed data
   *
   * @throws InterruptedException
   */
  @Test
  public void testTwoNodesShuffleSkew() throws InterruptedException {}

  /**
   * Create two Exchange operators and test that they correctly deal with uniform data
   *
   * @throws InterruptedException
   */
  @Test
  public void testTwoNodesShuffleUniform() throws InterruptedException {}

  /**
   * Create multiple Exchange operators and test that they correctly deal with two skewed input
   * relations
   *
   * @throws InterruptedException
   */
  @Test
  public void testMultipleNodeShuffleTwoSkewedInputs() throws InterruptedException {}
}
