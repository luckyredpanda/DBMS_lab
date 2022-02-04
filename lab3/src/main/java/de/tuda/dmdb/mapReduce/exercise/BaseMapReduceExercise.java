package de.tuda.dmdb.mapReduce.exercise;

import de.tuda.dmdb.mapReduce.Configuration;
import de.tuda.dmdb.mapReduce.Job;

/** An interface specifying basic operations for the MapReduce example tasks. */
public interface BaseMapReduceExercise {

  /**
   * Returns a job with the given configuration and with the correct Mapper/Reducer methods set.
   * Additionally the outputPrototype should be set.
   *
   * @param config the configuration to be used in the Job
   * @return a job for the given task.
   */
  Job createJob(Configuration config);
}
