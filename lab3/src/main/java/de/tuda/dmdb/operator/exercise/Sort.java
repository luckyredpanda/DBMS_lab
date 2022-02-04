package de.tuda.dmdb.operator.exercise;

import de.tuda.dmdb.operator.Operator;
import de.tuda.dmdb.operator.SortBase;
import de.tuda.dmdb.storage.AbstractRecord;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class Sort extends SortBase {


  public Sort(Operator child, Comparator<AbstractRecord> recordComparator) {
    super(child, recordComparator);
  }

  @Override
  public void open() {
    // TODO: implement this method
    // make sure to initialize the required (inherited) member variables
    this.child.open();
    this.sortedRecords=new PriorityQueue<>(recordComparator);
    AbstractRecord record;
    record = child.next();
    while (record != null) {
      sortedRecords.add(record);
      record=this.child.next();
    }
    System.out.println("sortedRecords"+sortedRecords);

  }

  @Override
  public AbstractRecord next() {
    // TODO: implement this method
    return sortedRecords.poll();

  }

  @Override
  public void close() {
    // TODO: implement this method
    // reverse what was done in open()
    this.child.close();
  }
}
