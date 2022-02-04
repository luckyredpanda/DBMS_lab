package de.tuda.dmdb.mapReduce.task;

import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.mapReduce.operator.ReducerBase;
import de.tuda.dmdb.storage.types.AbstractSQLValue;

/**
 * Base Class for implementing the Reducer Task Defines member variables The Reducer-Task is
 * executed during the reduce-phase and invokes the reduce-operator
 *
 * @author melhindi
 */
public class ReducerTaskBase extends MapReduceTask {

  /** This member variable defines the class of the reducer to instantiate in the run method */
  protected Class<
          ? extends
              ReducerBase<
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue>>
      reducerClass;

  /**
   * Constructor to create a task
   *
   * @param input - The input table on which this task will operate on
   * @param output - The output table to which this task write to
   * @param reducerClass - The java Class of the reducer to instantiate and use during the
   *     reduce-phase
   */
  public ReducerTaskBase(
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
    super(input, output);
    this.reducerClass = reducerClass;
  }
}
