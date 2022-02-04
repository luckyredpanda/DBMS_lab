package de.tuda.dmdb.mapReduce.task.exercise;

import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.mapReduce.operator.MapperBase;
import de.tuda.dmdb.mapReduce.operator.ReducerBase;
import de.tuda.dmdb.mapReduce.task.SinglePhaseTaskBase;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.operator.Exchange;
import de.tuda.dmdb.operator.SortBase;
import de.tuda.dmdb.operator.exercise.Sort;
import de.tuda.dmdb.operator.exercise.HashRepartitionExchange;
import java.util.Map;

/**
 * Defines what happens during the execution a map-reduce task Ie. implements the operator chains
 * for a complete map-reduce task We assume the same number of mappers and reducers (no need to
 * change hashFunction of shuffle) The Operator chain that this task implements is: Scan-{@literal
 * >}Mapper-{@literal >}Shuffle-{@literal >}Sort-{@literal >}Reducer The last operator in the chain
 * writes to the output, ie. is used to populate the output
 *
 * @author melhindi
 */
public class SinglePhaseTask extends SinglePhaseTaskBase {

  public SinglePhaseTask(
          HeapTable input,
          HeapTable output,
          int nodeId,
          Map<Integer, String> nodeMap,
          int partitionColumn,
          Class<
                  ? extends
                          MapperBase<
                                  ? extends AbstractSQLValue,
                                  ? extends AbstractSQLValue,
                                  ? extends AbstractSQLValue,
                                  ? extends AbstractSQLValue>>
                  mapperClass,
          Class<
                  ? extends
                          ReducerBase<
                                  ? extends AbstractSQLValue,
                                  ? extends AbstractSQLValue,
                                  ? extends AbstractSQLValue,
                                  ? extends AbstractSQLValue>>
                  reducerClass) {
    super(input, output, nodeId, nodeMap, partitionColumn, mapperClass, reducerClass);
  }

  @Override
  public void run() {
    // TODO: implement this method
    // read data from input (Remember: There is a special operator to read data from a Table)

    // define/instantiate the required operators
    TableScan ts = new TableScan(this.input);

    // define/instantiate the required operators
    // map
    try {
      MapperBase<? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue> mapper = this.mapperClass.newInstance();;
//    Mapper<AbstractSQLValue,AbstractSQLValue,AbstractSQLValue,AbstractSQLValue> mapper;
      mapper.setChild(ts);
      // shuffle
      Exchange exchange = new HashRepartitionExchange(mapper, this.nodeId, this.nodeMap, this.port, this.partitionColumn);
      // sort
      SortBase sort = new Sort(exchange, this.recordComparator);
      // reduce
      ReducerBase <? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue> reducer = this.reducerClass.newInstance();;
//    Reducer<AbstractSQLValue,AbstractSQLValue,AbstractSQLValue,AbstractSQLValue> reducer;
      reducer.setChild(sort);
      reducer.open();
      // write result to output
      AbstractRecord Record = reducer.next();
      while (Record != null)  {
        output.insert(Record);
        Record = reducer.next();
      }
      reducer.close();

    }
    catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    }
  }
}
