package de.tuda.dmdb.mapReduce.task;

import de.tuda.dmdb.access.exercise.HeapTable;

/**
 * Base task to define the structure of a task in out map-reduce execution model Input and output
 * are modeled as tables (and records) Each Task is executed on a node in the cluster (here: thread
 * pool)
 *
 * @author melhindi
 */
public class MapReduceTask extends Thread {
  protected HeapTable input; // provides access to the input data for this task
  protected HeapTable
      output; // provides access to the output location for this task, ie., a task can write to this
  // table to store the result for the client

  public MapReduceTask(HeapTable input, HeapTable output) {
    this.input = input;
    this.output = output;
  }

  public HeapTable getInput() {
    return input;
  }

  public void setInput(HeapTable input) {
    this.input = input;
  }

  public HeapTable getOutput() {
    return output;
  }

  public void setOutput(HeapTable output) {
    this.output = output;
  }
}
