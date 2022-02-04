package de.tuda.dmdb.mapReduce.task.exercise;

import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.mapReduce.task.ShuffleSortTaskBase;
import java.util.Map;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.operator.Exchange;
import de.tuda.dmdb.operator.SortBase;
import de.tuda.dmdb.operator.exercise.Sort;
import de.tuda.dmdb.operator.exercise.HashRepartitionExchange;


/**
 * Defines what happens during the shuffle+sort-phase of a map-reduce job Ie. implements the
 * operator chains for a shuffle+sort-phase The last operator in the chain writes to the output, ie.
 * is used to populate the output
 *
 * @author melhindi
 */
public class ShuffleSortTask extends ShuffleSortTaskBase {

  public ShuffleSortTask(
          HeapTable input,
          HeapTable output,
          int nodeId,
          Map<Integer, String> nodeMap,
          int partitionColumn,
          int numReducers) {
    super(input, output, nodeId, nodeMap, partitionColumn, numReducers);
  }

  @Override
  public void run() {
    // TODO: implement this method
    // read data from input (Remember: There is a special operator to read data from a Table)

    // define the shuffle-operator, account for a different number of reducers
    // define the sort operator, the base class already defines the comparator that you can use
    // process the input and write to the output

    TableScan ts = new TableScan(this.input);

    Exchange exchange= new HashRepartitionExchange(ts, this.nodeId, this.nodeMap, this.port, this.partitionColumn, this.numReducers);

    SortBase sort = new Sort(exchange, this.recordComparator);
    sort.open();

    AbstractRecord Record = sort.next();
    while (Record != null)  {
      output.insert(Record);
      Record = sort.next();
    }
    sort.close();
  }
}
