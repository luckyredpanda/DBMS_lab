package de.tuda.dmdb.mapReduce;

import de.tuda.dmdb.mapReduce.operator.MapReduceOperator;

/**
 * A map/reduce job configuration. Through the configuration the parameters for the job execution
 * are specified
 *
 * @author melhindi
 */
public class Configuration {
  // to set the number of reducers used for job execution
  // (it depends on the specific executor used, whether this value will be considered or not)
  protected int numReducers;
  //	the column used during re-partitioning in the shuffle phase
  protected int partitionColumn;
  // the first shuffler node will listen to that port, other shufflers will get a port++
  protected int startPort;

  /** Default Constructor, creates configuration with default values: */
  public Configuration() {
    this.numReducers = 0; // 0 will make the executor use the same number as for mappers
    this.partitionColumn = MapReduceOperator.KEY_COLUMN;
    this.startPort = 8000;
  }

  /**
   * Get the number of reducers set in a config object
   *
   * @return the number of reducers set in a config object
   */
  public int getNumReducers() {
    return numReducers;
  }

  /**
   * Set the number of reducers used for job execution Some Executors (e.g., SinglePhaseExecutor)
   * ignore this setting
   *
   * @param numReducers - Number of reducers as int
   */
  public void setNumReducers(int numReducers) {
    this.numReducers = numReducers;
  }

  public int getPartitionColumn() {
    return partitionColumn;
  }

  public void setPartitionColumn(int partitionColumn) {
    this.partitionColumn = partitionColumn;
  }

  public int getStartPort() {
    return startPort;
  }

  public void setStartPort(int startPort) {
    this.startPort = startPort;
  }
}
