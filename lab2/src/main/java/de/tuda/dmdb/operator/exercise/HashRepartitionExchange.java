package de.tuda.dmdb.operator.exercise;

import de.tuda.dmdb.operator.Exchange;
import de.tuda.dmdb.operator.Operator;
import de.tuda.dmdb.storage.types.EnumSQLType;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the Exchange operator used to implement hash-based repartitioning exchange
 *
 * @author melhindi
 */
public class HashRepartitionExchange extends Exchange {
  protected int
      numReceivers; // influences to how many nodes we will repartition the data too, default is all
  // nodes

  /**
   * Constructor to use a hash function that distributes records to all existing nodes
   *
   * @param child The input relation from which records are retrieved and potentially send to other
   *     nodes
   * @param nodeId Own nodeId used to identify local records in send-operator
   * @param nodeMap Map of the form NodeId:"IP:port" containing connection information of all peers
   *     in the network
   * @param listenerPort Port on which receive operator will listen for incoming connections
   * @param partitioningColumn Index of attribute/column in input relation that should be used when
   *     computing the hash partitioning
   */
  public HashRepartitionExchange(
      Operator child,
      int nodeId,
      Map<Integer, String> nodeMap,
      int listenerPort,
      int partitioningColumn) {
    this(child, nodeId, nodeMap, listenerPort, partitioningColumn, nodeMap.size());
  }

  /**
   * Constructor to use a hash function that distributes records to a subset of nodes (i.e. the
   * first X nodes in the network). Distributes the records among the nodes as '% numReceivers'
   *
   * @param child The input relation from which records are retrieved and potentially send to other
   *     nodes
   * @param nodeId Own nodeId used to identify local records in send-operator
   * @param nodeMap Map of the form NodeId:"IP:port" containing connection information of all peers
   *     in the network
   * @param listenerPort Port on which receive operator will listen for incoming connections
   * @param partitioningColumn Index of attribute/column in input relation that should be used when
   *     computing the hash partitioning
   * @param numReceivers Determines the subset of nodes that should be considered for the
   *     repartitioning (meaning the first numReceivers nodes in the network).
   */
  public HashRepartitionExchange(
      Operator child,
      int nodeId,
      Map<Integer, String> nodeMap,
      int listenerPort,
      int partitioningColumn,
      int numReceivers) {
    super(child, nodeId, nodeMap, listenerPort, null);
    this.numReceivers = numReceivers;
    // TODO: define and set the distribution function
    // use the corresponding SETTER method
    // Use modulo (%) numReceivers to determine to which node to send a record
    // You should support all the (two) possible data types, you can use hashCode()
    // on the value of a column
      
    this.distributionFunction =
            record -> {
              if (partitioningColumn >= record.getValues().length) {
                throw new IllegalArgumentException(
                        "Illegal partitioning Column, specified column number to high");
              }

              int value = 0;
              EnumSQLType type = record.getValue(partitioningColumn).getType();
              if (type == EnumSQLType.SqlInteger) {
                value = ((SQLInteger) record.getValue(partitioningColumn)).getValue();
              }
              if (type == EnumSQLType.SqlVarchar) {
                value = ((SQLVarchar) record.getValue(partitioningColumn)).getValue().hashCode();
              }
              List<Integer> result= new ArrayList<Integer>();
              //System.out.println(value % numReceivers);
              result.add(value % numReceivers);
              return result;
            };

  }
}
