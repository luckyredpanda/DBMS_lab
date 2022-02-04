package de.tuda.dmdb.operator.exercise;

import de.tuda.dmdb.operator.AbstractAggregationOperator;
import de.tuda.dmdb.operator.Operator;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.EnumSQLType;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Multi-purpose group-by aggregate operator
 *
 * @author melhindi
 */
public class GroupByAggregate extends AbstractAggregationOperator {
  // TODO: Define any required member variables for your operator
  int num=-1;
  int total = 0;
  int length;
  HashMap<AbstractRecord,AbstractRecord> bf = new HashMap<>();
  List<AbstractRecord> grouped = new ArrayList<>();


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
  public GroupByAggregate(
      Operator child,
      List<Integer> groupByAttributes,
      List<Integer> aggregateAttributes,
      List<BiFunction<Integer, Integer, Integer>> aggregateFunctions) {
    super(child, groupByAttributes, aggregateAttributes, aggregateFunctions);
  }

  @Override
  public void open() {
    // TODO implement this method
    // initialize member variables and child
    child.open();
    if (groupByAttributes!=null) length = groupByAttributes.size();
    num = 0;
    if (length==0) length=1;
  }

  @Override
  public AbstractRecord next() {
    // TODO implement this method
    // Consider the following: Is this a blocking or non-blocking operator?
    // groupByAttributes==null means no grouping required!
    if(child.next()==null) return null;
    AbstractRecord r = child.next();
    if (groupByAttributes == null){
      if (r==null) return null;
      AbstractRecord r1 = new Record(length);
      for (int i=0;i<length;i++)
        r1.setValue(i,new SQLInteger(1));
      while (r!=null) {
        for (int i=0;i<length;i++) {
          int update = r.getValue(aggregateAttributes.get(i)).compareTo(new SQLInteger(0));
          update = aggregateFunctions.get(i).apply(r1.getValue(i).compareTo(new SQLInteger(0)),update);
          r1.setValue(i,new SQLInteger(update));
        }
        r = child.next();
      }
      return r1;
    }
    else{
      System.out.println("length"+length);
      int newlength=(length+1)*length;
      AbstractRecord r1 = new Record(newlength);
      for (int i=0;i<length;i++)
        r1.setValue(0,new SQLInteger(1));
      while (r!=null) {
        for (int i=0;i<length;i++) {
          int update = r.getValue(aggregateAttributes.get(i)).compareTo(new SQLInteger(0));
          update = aggregateFunctions.get(i).apply(r1.getValue(i).compareTo(new SQLInteger(0)),update);
          r1.setValue(i+length*length,new SQLInteger(update));
        }
        r = child.next();
      }

      return r1;

    }
    //return r;
  }

  @Override
  public void close() {
    // TODO implement this method
    child.close();
  }
}
