package de.tuda.dmdb.operator;

import de.tuda.dmdb.net.TCPServer;
import de.tuda.dmdb.storage.AbstractRecord;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for implementation of receive operator
 *
 * @author melhindi
 */
public abstract class ReceiveBase extends UnaryOperator {

  protected int
      numPeers; // indicates number of peers that have to finish processing before operator
  // finishes
  protected int listenerPort; // port on which receive server will be listening
  protected Queue<AbstractRecord>
      localCache; // cache passed to receive handler for storing in-coming records
  protected TCPServer receiveServer; // used to store reference to receive server
  protected final AtomicInteger
      finishedPeers; // passed to receive server to track number of peers that finish data
  // transfer
  protected int nodeId; // own nodeId used for debugging

  /**
   * Constructor of ReceiveBase
   *
   * @param child - Child operator used to process next calls, usually SendOperator
   * @param numPeers - Number of peer nodes that have to finish processing before operator finishes
   * @param listenerPort - Port on which to bind receive server
   * @param nodeId - Own nodeId, used for debugging
   */
  public ReceiveBase(Operator child, int numPeers, int listenerPort, int nodeId) {
    super(child);
    this.numPeers = numPeers;
    this.listenerPort = listenerPort;
    this.finishedPeers = new AtomicInteger(1);
    this.nodeId = nodeId;
  }

  /**
   * Retrieve the localCache queue of the Receive Operator
   *
   * @return the localCache in which also remote records are received in
   */
  public Queue<AbstractRecord> getLocalCache() {
    return localCache;
  }

  /**
   * Set the localCache queue of the Receive Operator
   *
   * @param localCache the localCache to set
   */
  public void setLocalCache(Queue<AbstractRecord> localCache) {
    this.localCache = localCache;
  }
}
