package de.tuda.dmdb.operator.exercise;

import de.tuda.dmdb.operator.Exchange;
import de.tuda.dmdb.operator.Operator;
import de.tuda.dmdb.storage.AbstractRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the Exchange operator used to implement a 1:N exchange operator, i.e. a node
 * sends the records to all nodes in the network
 *
 * @author melhindi
 */
public class ReplicationExchange extends Exchange {

  /**
   * Constructor for a 1:N exchange operator, i.e. a node sends the records to all other nodes in
   * the network
   *
   * @param child The input relation from which records are retrieved and potentially send to other
   *     nodes
   * @param nodeId Own nodeId used to identify local records in send-operator
   * @param nodeMap Map of the form NodeId:"IP:port" containing connection information of all peers
   *     in the network
   * @param listenerPort Port on which receive operator will listen for incoming connections
   */
  public ReplicationExchange(
      Operator child, int nodeId, Map<Integer, String> nodeMap, int listenerPort) {
    super(child, nodeId, nodeMap, listenerPort, null);

    // TODO: define and set the distribution function
    // use the corresponding SETTER method
      this.distributionFunction =record -> {
          List<Integer> result = new ArrayList<>();
          for (Integer key : nodeMap.keySet()) {
              System.out.println(key);
              result.add(key);
          }
          return result;
          };

  }
}
