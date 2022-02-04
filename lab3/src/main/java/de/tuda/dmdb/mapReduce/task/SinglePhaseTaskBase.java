package de.tuda.dmdb.mapReduce.task;

import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.mapReduce.operator.MapperBase;
import de.tuda.dmdb.mapReduce.operator.ReducerBase;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import java.util.Comparator;
import java.util.Map;

/**
 * Base Class for implementing the MapReduce Task as an operator chain Defines member variables This
 * task chains Scan-{@literal >}mapper-{@literal >}shuffle-{@literal >}sort-{@literal >}reducer
 * operators to complete a map-reduce job
 *
 * @author melhindi
 */
public class SinglePhaseTaskBase extends MapReduceTask {
  protected int sortColumn;
  protected int nodeId;
  protected Map<Integer, String> nodeMap;
  protected int port;
  protected int partitionColumn;
  protected Comparator<AbstractRecord> recordComparator;

  protected Class<
          ? extends
              MapperBase<
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue>>
      mapperClass;
  protected Class<
          ? extends
              ReducerBase<
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue>>
      reducerClass;

  /**
   * Constructor to initialize all member variables
   *
   * @param input - The input table on which this task will operate on
   * @param output - The output table to which this task write to
   * @param nodeId - ID to passed through to the shuffle operator
   * @param nodeMap - Map passed through to the shuffle operator, defines connection information to
   *     peers
   * @param partitionColumn - Index of the column to use for re-partitioning during shuffling and
   *     for sorting
   * @param mapperClass - The java Class of the mapper to instantiate and use during the map-phase
   * @param reducerClass - The java Class of the reducer to instantiate and use during the
   *     reduce-phase
   */
  public SinglePhaseTaskBase(
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
    super(input, output);

    this.nodeId = nodeId;
    this.nodeMap = nodeMap;
    // determine port from nodeMap
    String[] conComponents = this.nodeMap.get(nodeId).split(":");
    this.port = Integer.parseInt(conComponents[1]);

    this.partitionColumn = partitionColumn;
    // create comparator for sort operator
    this.recordComparator =
        new Comparator<AbstractRecord>() {
          public int compare(AbstractRecord record1, AbstractRecord record2) {

            AbstractSQLValue value1 = record1.getValue(SinglePhaseTaskBase.this.partitionColumn);
            AbstractSQLValue value2 = record2.getValue(SinglePhaseTaskBase.this.partitionColumn);

            return value1.compareTo(value2);
          }
        };

    this.mapperClass = mapperClass;
    this.reducerClass = reducerClass;
  }
}
