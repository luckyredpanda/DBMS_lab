package de.tuda.dmdb.execution.exercise;

import de.tuda.dmdb.execution.QueryPlan;
import de.tuda.dmdb.operator.Operator;
import de.tuda.dmdb.operator.exercise.HashJoin;
import de.tuda.dmdb.operator.exercise.HashRepartitionExchange;
import java.util.Map;

/**
 * Query Subplan to perform a distributed asymmetric repartitioning join of two relations Assume
 * that the right relation will be repartitioned
 *
 * @author melhindi
 */
public class AsymmetricRepartitioningJoinPlan extends QueryPlan {
  protected Operator leftRelation;
  protected Operator rightRelation;
  int leftJoinAtt;
  int rightJoinAtt;
  int nodeId = 0;
  int listenerPort = 8000;
  Map<Integer, String> nodeMap;

  public AsymmetricRepartitioningJoinPlan(
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
    // Use a suited join algorithm
    HashRepartitionExchange operator_right = new HashRepartitionExchange(rightRelation,nodeId,nodeMap,listenerPort,rightJoinAtt);
    HashJoin operator = new HashJoin(leftRelation,operator_right,leftJoinAtt,rightJoinAtt);
    return operator;
  }
}
