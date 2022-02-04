package de.tuda.dmdb.operator.exercise;

import de.tuda.dmdb.operator.Operator;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.SQLNull;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;

import java.util.*;
import java.util.function.BiFunction;

/** Rollup aggregator for grouping by a set */
public class Rollup extends GroupByAggregate {
  // TODO: Define any required member variables for your operator
  int total = 0;
  boolean f = false;
  protected List<BiFunction<Integer, Integer, Integer>> combinationFunctions;

  /**
   * Rollup aggregate operator. Records are grouped and the passed aggregates are computed per group
   * and for each set removing the last grouping attribute. The returned record should include an
   * attribute for each column used for grouping as well as a column for each computed aggregate For
   * the additional records in rollup the columns being aggregated should have the value null
   *
   * @param child The input relation on which to perform the group-by-aggregate computation
   * @param groupByAttributes Index of the attributes/columns in the input relation that should be
   *     used for grouping records. Two records are in the same group if their values in all
   *     group-by columns are equal. The set should be rolled up from last to first, e.g. aggregates
   *     for (), (elem0), (elem0,elem1), ... for (elem0, elem1, ...) in this vector If null is
   *     passed no grouping should be performed
   * @param aggregateAttributes Index of the attributes/columns in the input relation that should be
   *     used to compute an aggregate, there is a 1:1 mapping of aggregateAttribute and
   *     aggregateFunction
   * @param aggregateFunctions List of aggregate functions to apply, thereby aggregateFunction at
   *     index 0 is applied on aggregateAttribute at index 0 in the aggregateAttributes
   * @param combinationFunctions List of aggregate functions to apply on intermediate aggregated
   *     values
   */
  public Rollup(
      Operator child,
      List<Integer> groupByAttributes,
      List<Integer> aggregateAttributes,
      List<BiFunction<Integer, Integer, Integer>> aggregateFunctions,
      List<BiFunction<Integer, Integer, Integer>> combinationFunctions) {
    super(child, groupByAttributes, aggregateAttributes, aggregateFunctions);
    this.combinationFunctions = combinationFunctions;
    if (aggregateAttributes.size() != combinationFunctions.size()) {
      throw new IllegalArgumentException(
          "For every aggregateAttribute you must define a corresponding combinationFunction");
    }
  }

  @Override
  public AbstractRecord next() {
    // TODO implement this method
    // Consider the following: Is this a blocking or non-blocking operator?
    // groupByAttributes==null means no grouping required!
    AbstractRecord r = child.next();
    int aggregateAttributes_c = aggregateAttributes.get(0);
    if (groupByAttributes == null){
      if (r == null) return null;
      AbstractRecord r1 = new Record(aggregateAttributes.size());
      for (int i=0;i<aggregateAttributes.size();i++)
        r1.setValue(i,new SQLInteger(0));
      while (r!=null) {
        for (int i=0;i<aggregateAttributes.size();i++) {
          int update = r.getValue(aggregateAttributes.get(i)).compareTo(new SQLInteger(0));
          update = aggregateFunctions.get(i).apply(r1.getValue(i).compareTo(new SQLInteger(0)),update);
          r1.setValue(i,new SQLInteger(update));
        }
        r = child.next();
      }
      return r1;
    }
    else{
      if (num!=0 && num == total) return null;
      while (r!=null) {
        AbstractRecord r1 = new Record(length);
        AbstractRecord r2 = new Record(length + aggregateAttributes.size());
        for (int i=0;i<length;i++) {
          AbstractSQLValue grouped_num = r.getValue(groupByAttributes.get(i));
          r1.setValue(i,grouped_num);
          r2.setValue(i,grouped_num);
        }
        if (bf.isEmpty() || !bf.containsKey(r1)) {
          grouped.add(r1);
          total ++;
          for (int i=0;i<length;i++)
            r2.setValue(i,r1.getValue(i));
          for (int i=0;i<aggregateAttributes.size();i++)
            r2.setValue(length+i, new SQLInteger(0));
          bf.put(r1,r2);
        }
        r2 = bf.get(r1);
        for (int i=0;i<aggregateAttributes.size();i++){
          int update = r.getValue(aggregateAttributes.get(i)).compareTo(new SQLInteger(0));
          update = aggregateFunctions.get(i).apply(r2.getValue(length+i).compareTo(new SQLInteger(0)),update);
          r2.setValue(length+i,new SQLInteger(update));
        }
        bf.put(r1,r2);
        r = child.next();
      }
      int m = total;
      if (!f) {
        for (int i=0;i<m;i++){
          AbstractRecord r_1 = bf.get(grouped.get(i));
          int n_roll = length;
          while (n_roll>0){
            AbstractRecord r1 = new Record(length);
            for (int k=0;k<n_roll-1;k++)
              r1.setValue(k,r_1.getValue(k));
            for (int k=n_roll-1;k<length;k++)
              r1.setValue(k,new SQLNull());
            AbstractRecord r2 = new Record(length + aggregateAttributes.size());
            if (bf.isEmpty() || !bf.containsKey(r1)){
              grouped.add(r1);
              total++;
              for (int j=0;j<length;j++)
                r2.setValue(j,r1.getValue(j));
              for (int j=0;j<aggregateAttributes.size();j++)
                r2.setValue(length+j, new SQLInteger(0));
              bf.put(r1,r2);
            }
            r2 = bf.get(r1);
            for (int j=0;j<aggregateAttributes.size();j++){
              int update = r_1.getValue(length+j).compareTo(new SQLInteger(0));
              update = combinationFunctions.get(j).apply(r2.getValue(length+j).compareTo(new SQLInteger(0)),update);
              r2.setValue(length+j,new SQLInteger(update));
            }
            bf.put(r1,r2);
            n_roll--;
          }
        }
        f=true;
      }
      AbstractRecord r1 = bf.get(grouped.get(num));
      System.out.println("Rollup "+r1);
      num ++;
      return r1;
    }
  }
}
