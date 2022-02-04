package de.tuda.dmdb.execution.exercise;

import de.tuda.dmdb.execution.QueryPlan;
import de.tuda.dmdb.operator.Operator;
import de.tuda.dmdb.operator.exercise.GatherExchange;
import de.tuda.dmdb.operator.exercise.GroupByAggregate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.function.BiFunction;

/**
 * Query Subplan to perform a distributed count aggregation
 *
 * @author melhindi
 */
public class DistributedCountPlan extends QueryPlan {
  protected Operator inputRelation;
  int leftJoinAtt;
  int rightJoinAtt;
  int nodeId = 0;
  int listenerPort = 8000;
  Map<Integer, String> nodeMap;
  protected int coordinatorId;
  protected Vector<Integer> groupByAttributes;
  protected Vector<Integer> aggregateAttributes; // assume this only contains one entry

  public DistributedCountPlan(
      Operator inputRelation,
      int nodeId,
      int listenerPort,
      Map<Integer, String> nodeMap,
      int coordinatorId,
      Vector<Integer> groupByAttributes,
      Vector<Integer> aggregateAttributes) {
    this.inputRelation = inputRelation;
    this.nodeId = nodeId;
    this.listenerPort = listenerPort;
    this.nodeMap = nodeMap;
    this.coordinatorId = coordinatorId;
    this.groupByAttributes = groupByAttributes;
    this.aggregateAttributes = aggregateAttributes;
  }

  @Override
  public Operator compilePlan() {
    // TODO: Define plan by chaining together operators and return root operator
    List<BiFunction<Integer, Integer, Integer>> aggregateFunctions = new ArrayList<>();
    aggregateFunctions.add(
            (old, update) -> {
              if (old == null || old == 0) {
                return 1;
              }
              return old + 1;
            });
    List<BiFunction<Integer, Integer, Integer>> aggregateFunctions_g = new ArrayList<>();
    aggregateFunctions_g.add(
            (old, update) -> {
              if (old == null) {
                return update;
              }
              return old + update;
            });
    GroupByAggregate group_local =
            new GroupByAggregate(inputRelation, groupByAttributes,aggregateAttributes,aggregateFunctions);

    GatherExchange exchange = new GatherExchange(group_local,nodeId,nodeMap,listenerPort,coordinatorId);
    Vector<Integer> groupByAttributes_g = new Vector<>();
    Vector<Integer> aggregateAttributes_g = new Vector<>();

    if (groupByAttributes == null) {
      groupByAttributes_g = null;
      aggregateAttributes_g.add(0);
    }
    else {
      for (int i=0;i<groupByAttributes.size();i++) groupByAttributes_g.add(i);
      aggregateAttributes_g.add(groupByAttributes.size());
    }
    GroupByAggregate group_global =
            new GroupByAggregate(exchange, groupByAttributes_g,aggregateAttributes_g,aggregateFunctions_g);
    System.out.println("global"+group_global);
    System.out.println("local"+group_local);

    return group_global;
  }
}
