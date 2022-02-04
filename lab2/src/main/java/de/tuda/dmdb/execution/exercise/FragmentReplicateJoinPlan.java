package de.tuda.dmdb.execution.exercise;

import de.tuda.dmdb.execution.QueryPlan;
import de.tuda.dmdb.operator.Operator;
import de.tuda.dmdb.operator.exercise.HashJoin;
import de.tuda.dmdb.operator.exercise.ReplicationExchange;
import java.util.Map;

/**
 * Query Subplan to perform a distributed fragment-replicate join of two relations Assume that the
 * left relation is the smaller relation
 *
 * @author melhindi
 */
public class FragmentReplicateJoinPlan extends QueryPlan {
  protected Operator leftRelation;
  protected Operator rightRelation;
  int leftJoinAtt;
  int rightJoinAtt;
  int nodeId = 0;
  int listenerPort = 8000;
  Map<Integer, String> nodeMap;

  public FragmentReplicateJoinPlan(
      Operator leftRelation,
      Operator rightRelation,
      int leftJoinAtt,
      int rightJoinAtt,
      int nodeId,
      int listenerPort,
      Map<Integer, String> nodeMap) {
    this.leftRelation = leftRelation;
    this.rightRelation = rightRelation;
    this.leftJoinAtt = leftJoinAtt;
    this.rightJoinAtt = rightJoinAtt;
    this.nodeId = nodeId;
    this.listenerPort = listenerPort;
    this.nodeMap = nodeMap;
  }

  @Override
  public Operator compilePlan() {
    // TODO: Define plan by chaining together operators and return root operator
    ReplicationExchange exchange_left = new ReplicationExchange(leftRelation, nodeId, nodeMap, listenerPort);
    HashJoin operator = new HashJoin(exchange_left,rightRelation,leftJoinAtt,rightJoinAtt);
    return operator;
  }
}
