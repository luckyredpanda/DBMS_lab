package de.tuda.dmdb.operator;

import java.util.List;
import java.util.function.BiFunction;

public abstract class AbstractAggregationOperator extends UnaryOperator {

  // the attributes of the record that should be used for grouping
  protected List<Integer> groupByAttributes;

  /**
   * The attributes that should be used to compute an aggregate, there is a 1:1 mapping of
   * aggregateAttribute and aggregateFunction is applied on aggregateAttribute at index 0 in the
   * aggregateAttributes
   */
  protected List<Integer> aggregateAttributes;

  // List of aggregate functions to apply
  protected List<BiFunction<Integer, Integer, Integer>> aggregateFunctions;

  /**
   * Multi-purpose group-by aggregate operator. Records are grouped and the passed aggregates are
   * computed per group. The returned record should include an attribute for each column used for
   * grouping as well as a column for each computed aggregate
   *
   * @param child The input relation on which to perform the group-by-aggregate computation
   * @param groupByAttributes Index of the attributes/columns in the input relation that should be
   *     used for grouping records. Two records are in the same group if their values in all
   *     group-by columns are equal. If null is passed no grouping should be performed
   * @param aggregateAttributes Index of the attributes/columns in the input relation that should be
   *     used to compute an aggregate, there is a 1:1 mapping of aggregateAttribute and
   *     aggregateFunction
   * @param aggregateFunctions List of aggregate functions to apply, thereby aggregateFunction at
   *     index 0 is applied on aggregateAttribute at index 0 in the aggregateAttributes
   */
  public AbstractAggregationOperator(
      Operator child,
      List<Integer> groupByAttributes,
      List<Integer> aggregateAttributes,
      List<BiFunction<Integer, Integer, Integer>> aggregateFunctions) {
    super(child);
    this.groupByAttributes = groupByAttributes;
    this.aggregateAttributes = aggregateAttributes;
    this.aggregateFunctions = aggregateFunctions;
    if (aggregateAttributes.size() != aggregateFunctions.size()) {
      throw new IllegalArgumentException(
          "For every aggregateAttribute you must define a corresponding aggregateFunction");
    }
  }

  /**
   * Retrieve the aggregate functions that are applied by this operator Getter method used for
   * testing and debugging purposes
   *
   * @return the aggregateFunctions
   */
  public List<BiFunction<Integer, Integer, Integer>> getAggregateFunctions() {
    return aggregateFunctions;
  }
}
