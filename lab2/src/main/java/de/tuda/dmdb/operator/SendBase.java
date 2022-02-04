package de.tuda.dmdb.operator;

import de.tuda.dmdb.net.TCPClient;
import de.tuda.dmdb.storage.AbstractRecord;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Base class for implementation of send operator
 *
 * @author melhindi
 */
public abstract class SendBase extends UnaryOperator {

  protected int nodeId; // own nodeId to identify which records to keep locally
  protected Map<Integer, String>
      nodeMap; // map containing connection information to establish connection to other
  // peers
  protected Map<Integer, TCPClient> connectionMap; // map to store connection to other peers
  protected Function<AbstractRecord, List<Integer>>
      distributionFunction; // function that will be used by the send
  // operator to determine where to send a
  // record to

  /**
   * Constructor of SendBase
   *
   * @param child Child operator used to process next calls, e.g., TableScan or Selection
   * @param nodeId Own nodeId to identify which records to keep locally
   * @param nodeMap Map containing connection information (as "IP:port" or "domain-name:port") to
   *     establish connection to other peers
   * @param distributionFunction Function to use to determine the nodes (ie. nodeIds) to send a
   *     record to
   */
  public SendBase(
      Operator child,
      int nodeId,
      Map<Integer, String> nodeMap,
      Function<AbstractRecord, List<Integer>> distributionFunction) {
    super(child);
    this.nodeId = nodeId;
    this.nodeMap = nodeMap;
    this.connectionMap = new HashMap<Integer, TCPClient>();
    this.distributionFunction = distributionFunction;
  }

  /** Adequately close all connection that where established to peers during init phase */
  protected void closeConnectionsToPeers() {
    Iterator<Integer> it = this.connectionMap.keySet().iterator();
    while (it.hasNext()) {
      TCPClient connection = this.connectionMap.get(it.next());
      if (connection != null) {
        connection.close();
      } else {
        System.out.println(
            "SendBase: WARNING: No connection to close for peer " + this.nodeMap.get(nodeId));
      }
      it.remove(); // avoids a ConcurrentModificationException
    }
  }

  /**
   * Get the distribution function currently used by the send operator
   *
   * @return the distributionFunction
   */
  public Function<AbstractRecord, List<Integer>> getDistributionFunction() {
    return distributionFunction;
  }

  /**
   * Set the distribution function used by the send operator
   *
   * @param distributionFunction the distributionFunction to set
   */
  public void setDistributionFunction(
      Function<AbstractRecord, List<Integer>> distributionFunction) {
    this.distributionFunction = distributionFunction;
  }
}
