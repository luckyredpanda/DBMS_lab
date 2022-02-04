package de.tuda.dmdb.mapReduce;

import de.tuda.dmdb.access.exercise.HeapTable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Base Executor class that is extended by other executors It prepares and creates the environment
 * required to run the map-reduce job The init() method must be called to prepare the execution of
 * job
 *
 * @author melhindi
 */
public abstract class MapReduceExecutor {
  protected int
      numMappers; // number of mappers used, usually same as number of input-partitions passed in by
  // the user
  protected int
      numReducers; // number of reducers used, can be ignored by different executor implementations
  protected ExecutorService cluster; // java thread pool used as cluster
  protected List<Future<?>>
      tasks; // each Future for a specific thread submitted to the pool, used to track task/phase
  // completion
  protected boolean completed = false; // indicates that the job execution has completed

  protected Map<Integer, String> nodeMap; // used to initialize shuffle connections for the nodes

  /**
   * Submit job to the cluster
   *
   * @param job - The job to submit
   */
  public abstract void submit(Job job);

  /**
   * Initializes member variables and thus prepare the execution environment The number of mappers
   * and reducers is determine based on information in the job
   *
   * @param job - the job that has been submitted
   */
  protected void init(Job job) {
    this.completed = false;
    // create ThreadPools
    this.numMappers = job.getInputs().size(); // we assume one mapper for each partition
    this.numReducers = job.getConfiguration().getNumReducers(); // first: read value from config
    if (0
        == this
            .numReducers) { // second: if value was not set by user, we default to the same number
      // as mappers
      this.numReducers = this.numMappers;
      // pass back to configuration so it can been seen how many reducers were used
      job.getConfiguration().setNumReducers(this.numReducers);
    }

    // we use a fixed size thread pool, default: one thread (node) for each mapper
    this.cluster = Executors.newFixedThreadPool(this.numMappers);

    this.tasks = new LinkedList<Future<?>>();

    // create nodeMap for shuffle phase
    int startPort = job.getConfiguration().getStartPort();
    this.nodeMap = new HashMap<Integer, String>();
    for (int i = 0; i < this.numMappers; i++) {
      nodeMap.put(i, "localhost:" + (startPort + i));
    }

    // init output if required
    if (job.getOutputs().size() < this.numMappers) {
      job.getOutputs().clear();
      for (int i = 0; i < this.numMappers; i++) {
        // create empty HeapTables with correct record format
        job.getOutputs().add(new HeapTable(job.getOutputPrototype()));
      }
    }
  }

  /**
   * Determine if the execution of the job has completed
   *
   * @return True if execution completed otherwise false
   */
  public boolean isExecutionCompleted() {
    return this.completed;
  }

  /**
   * Wait for completion of all tasks (= usually threads) in the cluster Current implementation is
   * very simplified
   */
  protected void waitForCompletion() {
    long startTime = System.nanoTime();
    System.out.println("MapReduceExecutor: Waiting for completion of tasks...");
    Iterator<Future<?>> it =
        this.tasks.iterator(); // use iterator to be able to remove the task once it finished
    while (it.hasNext()) {
      Future<?> future = it.next();
      try {
        future.get(); // this is blocking and only returns if future finished
        it.remove();
      } catch (Exception e) {
        e.printStackTrace();
        System.err.println("MapReduceExecutor: Error while waiting for thread completion");
      }
    }
    long difference = System.nanoTime() - startTime;
    System.out.println(
        "MapReduceExecutor: Tasks completed! Took: "
            + TimeUnit.NANOSECONDS.toMicros(difference)
            + " microsecs");
  }
}
