package de.tuda.dmdb.mapReduce.task.exercise;

import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.mapReduce.operator.ReducerBase;
import de.tuda.dmdb.mapReduce.operator.exercise.Reducer;
import de.tuda.dmdb.mapReduce.task.ReducerTaskBase;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.storage.AbstractRecord;

/**
 * Defines what happens during the reduce-phase of a map-reduce job Ie. implements the operator
 * chains for a reduce-phase The last operator in the chain writes to the output, ie. is used to
 * populate the output
 *
 * @author melhindi
 */
public class ReducerTask extends ReducerTaskBase {

  public ReducerTask(
          HeapTable input,
          HeapTable output,
          Class<
                  ? extends
                          ReducerBase<
                                  ? extends AbstractSQLValue,
                                  ? extends AbstractSQLValue,
                                  ? extends AbstractSQLValue,
                                  ? extends AbstractSQLValue>>
                  reducerClass) {
    super(input, output, reducerClass);
  }

  @Override
  public void run() {
    // TODO: implement this method
    // read data from input (Remember: There is a special operator to read data from a Table)

    TableScan ts = new TableScan(this.input);

    // instantiate the reduce
    try {
      ReducerBase <? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue> reducer = this.reducerClass.newInstance();
//    Reducer<AbstractSQLValue,AbstractSQLValue,AbstractSQLValue,AbstractSQLValue> reducer;
//    reducer = new Reducer<>();
      reducer.setChild(ts);
      reducer.open();
      // write result to output
      AbstractRecord Record = reducer.next();
      while (Record != null)  {
        output.insert(Record);
        Record = reducer.next();
      }
      reducer.close();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }
}
