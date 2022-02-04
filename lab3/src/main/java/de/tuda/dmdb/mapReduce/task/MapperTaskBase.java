package de.tuda.dmdb.mapReduce.task;

import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.mapReduce.operator.MapperBase;
import de.tuda.dmdb.storage.types.AbstractSQLValue;

/**
 * Base Class for implementing the Mapper Task Defines member variables The Mapper-Task is executed
 * during the map-phase and invokes the mapper-operator
 *
 * @author melhindi
 */
public class MapperTaskBase extends MapReduceTask {

  /** This member variable defines the class of the mapper to instantiate in the run method */
  protected Class<
          ? extends
              MapperBase<
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue>>
      mapperClass;

  /**
   * Constructor to create a task
   *
   * @param input - The input table on which this task will operate on
   * @param output - The output table to which this task write to
   * @param mapperClass - The java Class of the mapper to instantiate and use during the map-phase
   */
  public MapperTaskBase(
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
    super(input, output);
    this.mapperClass = mapperClass;
  }
}
