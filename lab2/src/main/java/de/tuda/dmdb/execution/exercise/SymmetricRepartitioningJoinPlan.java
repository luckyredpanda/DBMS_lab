package de.tuda.dmdb.execution.exercise;

import de.tuda.dmdb.execution.QueryPlan;
import de.tuda.dmdb.operator.Operator;
import de.tuda.dmdb.operator.exercise.HashJoin;
import de.tuda.dmdb.operator.exercise.HashRepartitionExchange;
import de.tuda.dmdb.operator.exercise.ReplicationExchange;

import java.util.Map;

/**
 * Query Subplan to perform a distributed asymmetric repartitioning join of two relations Both
 * relations need to be repartitioned on the join-key
 *
 * @author melhindi
 */
public class SymmetricRepartitioningJoinPlan extends QueryPlan {
  protected Operator leftRelation;
  protected Operator rightRelation;
  int leftJoinAtt;
  int rightJoinAtt;
  int nodeId = 0;
  int listenerPortLeft = 8000;
  int listenerPortRight = this.listenerPortLeft + 1;
  Map<Integer, String> nodeMapLeft;
  Map<Integer, String> nodeMapRight;

  public SymmetricRepartitioningJoinPlan(
      Operator leftRelation,
      Operator rightRelation,
      int leftJoinAtt,
      int rightJoinAtt,
      int nodeId,
      int listenerPortLeft,
      Map<Integer, String> nodeMapLeft,
      int listenerPortRight,
      Map<Integer, String> nodeMapRight) {
    this.leftRelation = leftRelation;
    this.rightRelation = rightRelation;
    this.leftJoinAtt = leftJoinAtt;
    this.rightJoinAtt = rightJoinAtt;
    this.nodeId = nodeId;
    this.listenerPortLeft = listenerPortLeft;
    this.listenerPortRight = listenerPortRight;
    this.nodeMapLeft = nodeMapLeft;
    this.nodeMapRight = nodeMapRight;
  }

  @Override
  public Operator compilePlan() {
    // TODO: Define plan by chaining together operators and return root operator
    // Use a suitable join algorithm
    //ReplicationExchange operator_left = new ReplicationExchange(leftRelation,nodeId,this.nodeMapLeft,listenerPortLeft);
    //ReplicationExchange operator_right = new ReplicationExchange(rightRelation,nodeId,this.nodeMapRight,listenerPortRight);
    //HashJoin operator = new HashJoin(operator_left,rightRelation,leftJoinAtt,rightJoinAtt);
    HashRepartitionExchange operator_left = new HashRepartitionExchange(leftRelation,nodeId,this.nodeMapLeft,listenerPortLeft,rightJoinAtt);
    HashRepartitionExchange operator_right = new HashRepartitionExchange(rightRelation,nodeId,this.nodeMapRight,listenerPortRight,leftJoinAtt);
    HashJoin operator = new HashJoin(operator_left,operator_right,leftJoinAtt,rightJoinAtt);
    return operator;
  }
}
