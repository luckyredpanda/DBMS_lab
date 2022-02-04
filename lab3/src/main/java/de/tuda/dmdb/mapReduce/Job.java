package de.tuda.dmdb.mapReduce;

import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.mapReduce.operator.exercise.Mapper;
import de.tuda.dmdb.mapReduce.operator.exercise.Reducer;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import java.util.List;

/**
 * Similar to
 * https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/Job.java
 *
 * <p>The job executors's view of the Job.
 *
 * <p>It allows the user to configure the job, submit it, control its execution, and query the
 * state. The set methods only work until the job is submitted
 *
 * <p>Normally the user creates the application, describes various facets of the job via {@link Job}
 * and then submits the job and monitor its progress.
 *
 * <p>Here is an example on how to submit a job:
 *
 * <blockquote>
 *
 * <pre>
 *     // Create a new Job
 *     Configuration conf = new Configuration();
 * 	Job job = Job.getInstance(conf, "word count");
 * 	job.setMapperClass(TokenizerMapper.class);
 * 	job.setReducerClass(IntSumReducer.class);
 *       // inputs and ouputs are HeapTables - no paths/files are supported
 * 	job.setInputs(partitions);
 * 	job.setOutputs(outputs);
 * 	job.setOutputPrototype(outputPrototype);
 *
 *     // Submit the job, then poll for progress until the job is complete
 *     job.waitForCompletion(true);
 * </pre>
 *
 * </blockquote>
 *
 * @author melhindi
 */
public class Job {

  protected String name; // name/description of the jon
  protected Configuration configuration;
  // the input and output for a job is modelled as a table not path/file
  // e.g., multiple input files are passed in as HeapTables
  // the record format in a table is <key, value>
  // the key & value types for the input must correspond with what is expected by the mapper
  // the key & value types for the output must correspond with what is expected by the reducer
  protected List<HeapTable> inputs;
  protected List<HeapTable> outputs;
  protected AbstractRecord
      outputPrototype; // used by the executor to init the ouput, must correspond to what is
  // expected by the reducer

  protected MapReduceExecutor executor; // the executer to whoem the job will be submitted

  protected int state = 0;

  protected Class<
          ? extends
              Mapper<
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue>>
      mapperClass;
  protected Class<
          ? extends
              Reducer<
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue>>
      reducerClass;

  public Job(Configuration conf, String name) {
    this.name = name;
    this.configuration = conf;
    this.executor = new SinglePhaseExecutor(); // set default executor
  }

  /**
   * Creates a new {@link Job} with a given jobName. A Cluster will be created from the conf
   * parameter only when it's needed.
   *
   * <p>The <code>Job</code> modifies the <code>Configuration</code> so that any changes are visible
   * to the user.
   *
   * @param conf the configuration
   * @param name The name of the job
   * @return the {@link Job}.
   */
  public static Job getInstance(Configuration conf, String name) {
    return new Job(conf, name);
  }

  /**
   * Submit the job to the cluster. The executor method blocks, at the moment, so this method will
   * also block
   *
   * @throws InterruptedException if blocking execution has been interrupted
   */
  public void submit() throws InterruptedException {
    this.state = 1;

    executor.submit(this);
  }

  /**
   * Submit the job to the cluster and wait for it to finish.
   *
   * @return true if the job succeeded
   */
  public boolean waitForCompletion() {
    if (this.state == 0) {
      try {
        submit();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        return false;
      }
    }
    // the submit method is currently, blocking... this code is just a dummy/placeholder
    while (!this.isComplete()) {
      try {
        int sleepTime = 1000;
        System.out.println("Waiting for job to complete, sleeping " + sleepTime + "millisecs!");
        Thread.sleep(sleepTime);
      } catch (InterruptedException ie) {
        return false;
      }
    }
    System.out.println("Job '" + this.name + "' completed!");

    return true;
  }

  /**
   * Check if the job is finished or not. This is a non-blocking call.
   *
   * @return <code>true</code> if the job is complete, else <code>false</code>.
   */
  public boolean isComplete() {
    return this.executor.isExecutionCompleted();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }

  public Class<
          ? extends
              Mapper<
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue>>
      getMapperClass() {
    return mapperClass;
  }

  public void setMapperClass(
      Class<
              ? extends
                  Mapper<
                      ? extends AbstractSQLValue,
                      ? extends AbstractSQLValue,
                      ? extends AbstractSQLValue,
                      ? extends AbstractSQLValue>>
          mapperClass) {
    this.mapperClass = mapperClass;
  }

  public Class<
          ? extends
              Reducer<
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue,
                  ? extends AbstractSQLValue>>
      getReducerClass() {
    return reducerClass;
  }

  public void setReducerClass(
      Class<
              ? extends
                  Reducer<
                      ? extends AbstractSQLValue,
                      ? extends AbstractSQLValue,
                      ? extends AbstractSQLValue,
                      ? extends AbstractSQLValue>>
          reducerClass) {
    this.reducerClass = reducerClass;
  }

  public List<HeapTable> getInputs() {
    return inputs;
  }

  public void setInputs(List<HeapTable> inputs) {
    this.inputs = inputs;
  }

  public List<HeapTable> getOutputs() {
    return outputs;
  }

  public void setOutputs(List<HeapTable> outputs) {
    this.outputs = outputs;
  }

  public AbstractRecord getOutputPrototype() {
    return outputPrototype;
  }

  public void setOutputPrototype(AbstractRecord outputPrototype) {
    this.outputPrototype = outputPrototype.clone();
  }

  public MapReduceExecutor getExecutor() {
    return executor;
  }

  public void setExecutor(MapReduceExecutor executor) {
    this.executor = executor;
  }

  /**
   * Set the number of reduce tasks for the job.
   *
   * @param tasks the number of reduce tasks
   */
  public void setNumReduceTasks(int tasks) {
    this.configuration.setNumReducers(tasks);
  }
}
