package de.tuda.dmdb.mapReduce;

import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.mapReduce.task.exercise.MapperTask;
import de.tuda.dmdb.mapReduce.task.exercise.ReducerTask;
import de.tuda.dmdb.mapReduce.task.exercise.ShuffleSortTask;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;

/**
 * Executes map-reduce job as multiple phases and write out intermediate results after each phase
 * The three phases map, shuffle+sort, reduce are executed sequentially
 *
 * @author melhindi
 */
public class MultiPhaseExecutor extends MapReduceExecutor {

  @Override
  public void submit(Job job) {
    init(job); // prepare execution

    // to store intermediate results
    List<HeapTable> mapperResults = new ArrayList<HeapTable>();
    List<HeapTable> shuffleResults = new ArrayList<HeapTable>();

    // run phases
    // tasks are added to the tasks memeber to be able to track progress
    // the call to waitForCompletion() (blocking) removes the finished tasks from the tasks member

    // run map phase
    for (HeapTable partition : job.inputs) {
      HeapTable mapperResult = new HeapTable(job.getOutputPrototype());
      mapperResults.add(mapperResult);
      Future<?> future =
          cluster.submit(new MapperTask(partition, mapperResult, job.getMapperClass()));
      tasks.add(future);
    }

    // wait for phase to finish
    this.waitForCompletion();

    // sort&shuffle phase
    for (int i = 0; i < this.numMappers; i++) {
      HeapTable shuffleResult = new HeapTable(job.getOutputPrototype());
      shuffleResults.add(shuffleResult);
      Future<?> future =
          cluster.submit(
              new ShuffleSortTask(
                  mapperResults.get(i),
                  shuffleResult,
                  i,
                  this.nodeMap,
                  job.getConfiguration().getPartitionColumn(),
                  this.numReducers));
      tasks.add(future);
    }

    // wait for phase to finish
    this.waitForCompletion();

    // invoke reduce phase for non-empty shuffleResults
    Iterator<HeapTable> it = shuffleResults.iterator();
    while (it.hasNext()) {
      HeapTable heapTable = it.next();
      if (heapTable.getRecordCount() == 0) {
        it.remove();
      }
    }

    for (int i = 0; i < numReducers; i++) {
      Future<?> future =
          cluster.submit(
              new ReducerTask(
                  shuffleResults.get(i), job.getOutputs().get(i), job.getReducerClass()));
      tasks.add(future);
    }

    // wait for phase to finish
    this.waitForCompletion();

    // remove empty result tables - this is the case when numReducers < numMappers
    Iterator<HeapTable> resultIter = job.getOutputs().iterator();
    while (resultIter.hasNext()) {
      HeapTable heapTable = resultIter.next();
      if (heapTable.getRecordCount() == 0) {
        resultIter.remove();
      }
    }

    // make sure to updated state (ie, 'completed' member) to reflect that execution has finished
    this.completed = true;
  }
}
