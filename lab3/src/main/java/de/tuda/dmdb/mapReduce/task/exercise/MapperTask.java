package de.tuda.dmdb.mapReduce.task.exercise;

import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.mapReduce.operator.MapperBase;
import de.tuda.dmdb.mapReduce.operator.exercise.Mapper;
import de.tuda.dmdb.mapReduce.task.MapperTaskBase;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.storage.AbstractRecord;

/**
 * Defines what happens during the map-phase of a map-reduce job Ie. implements the operator chains
 * for a reduce-phase The last operator in the chain writes to the output, ie. is used to populate
 * the output
 *
 * @author melhindi
 */
public class MapperTask extends MapperTaskBase {

  public MapperTask(
          HeapTable input,
          HeapTable output,
          Class<
                  ? extends
                          MapperBase<
                                  ? extends AbstractSQLValue,
                                  ? extends AbstractSQLValue,
                                  ? extends AbstractSQLValue,
                                  ? extends AbstractSQLValue>>
                  mapperClass) {
    super(input, output, mapperClass);
  }

  @Override
  public void run() {
    // TODO: implement this method
    // read data from input (Remember: There is a special operator to read data from a Table)

    TableScan ts = new TableScan(input);

    // instantiate the mapper
    try {
      MapperBase<? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue> mapper = this.mapperClass.newInstance();
//    Mapper<AbstractSQLValue,AbstractSQLValue,AbstractSQLValue,AbstractSQLValue> mapper=new MapperTask();
      mapper.setChild(ts);
      mapper.open();
      // write result to output
      AbstractRecord Record = mapper.next();
      while (Record != null)  {
        output.insert(Record);
        Record = mapper.next();
      }
      mapper.close();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }
}
