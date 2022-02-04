package de.tuda.dmdb.operator.exercise;

import de.tuda.dmdb.operator.Exchange;
import de.tuda.dmdb.operator.Operator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the Exchange operator used to implement a N:1 exchange operator, i.e. multiple
 * nodes are sending to the same node (called coordinator)
 *
 * @author melhindi
 */
public class GatherExchange extends Exchange {
  protected int coordinatorId; // id of node to which other nodes will send records

  /**
   * Constructor for a N:1 exchange operator, i.e. multiple nodes are sending to the same node
   * (called coordinator)
   *
   * @param child The input relation from which records are retrieved and potentially send to other
   *     nodes
   * @param nodeId Own nodeId used to identify local records in send-operator
   * @param nodeMap Map of the form NodeId:"IP:port" containing connection information of all peers
   *     in the network
   * @param listenerPort Port on which receive operator will listen for incoming connections
   * @param coordinatorId Id of the node to which all records should be sent
   */
  public GatherExchange(
      Operator child,
      int nodeId,
      Map<Integer, String> nodeMap,
      int listenerPort,
      int coordinatorId) {
    super(child, nodeId, nodeMap, listenerPort,null);
    this.coordinatorId = coordinatorId;
    // TODO: define and set the distribution function to implement the N:1 behavior
    // of the operator, i.e., all records should be sent to the coordinator
    // use the corresponding SETTER method
    this.distributionFunction =
            record -> {
            List<Integer> result= new ArrayList<>();
            result.add(this.coordinatorId);
            return result;
            };

    //this.receiveOperator.open();

  }
}
