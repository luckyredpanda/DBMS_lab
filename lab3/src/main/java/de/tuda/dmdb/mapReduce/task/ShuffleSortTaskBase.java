package de.tuda.dmdb.mapReduce.task;

import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import java.util.Comparator;
import java.util.Map;

/**
 * Base Class for implementing the Shuffle+Sort Task Defines member variables The Shuffle+Sort-Task
 * is executed during the shuffle-phase and invokes the shuffle and sort operators
 *
 * @author melhindi
 */
public class ShuffleSortTaskBase extends MapReduceTask {
  // the following memeber variables are required for the shuffle&sort operators
  protected int nodeId; // ID to passed through to the shuffle operator
  protected Map<Integer, String>
      nodeMap; // Map passed through to the shuffle operator, defines connection information to
  // peers
  protected int port; // Port which shuffle operator will listen to, is determined from the nodeMap
  protected int
      partitionColumn; // Index of the column to use for re-partitioning during shuffling and for
  // sorting
  protected int numReducers; // Number of reducer to use, this determines the hash function
  protected Comparator<AbstractRecord> recordComparator; // used for sorting the records

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
   * @param numReducers - Number of reducer to use
   */
  public ShuffleSortTaskBase(
      HeapTable input,
      HeapTable output,
      int nodeId,
      Map<Integer, String> nodeMap,
      int partitionColumn,
      int numReducers) {
    super(input, output);

    this.nodeId = nodeId;
    this.nodeMap = nodeMap;
    // determine port from nodeMap
    String[] conComponents = this.nodeMap.get(nodeId).split(":");
    this.port = Integer.parseInt(conComponents[1]);

    this.partitionColumn = partitionColumn;
    this.numReducers = numReducers;
    // create comparator for sort operator
    this.recordComparator =
        new Comparator<AbstractRecord>() {
          public int compare(AbstractRecord record1, AbstractRecord record2) {

            AbstractSQLValue value1 = record1.getValue(ShuffleSortTaskBase.this.partitionColumn);
            AbstractSQLValue value2 = record2.getValue(ShuffleSortTaskBase.this.partitionColumn);

            return value1.compareTo(value2);
          }
        };
  }
}
