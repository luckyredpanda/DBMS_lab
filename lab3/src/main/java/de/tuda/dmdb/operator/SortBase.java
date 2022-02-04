package de.tuda.dmdb.operator;

import de.tuda.dmdb.storage.AbstractRecord;
import java.util.Comparator;
import java.util.PriorityQueue;

public abstract class SortBase extends UnaryOperator {
  protected PriorityQueue<AbstractRecord> sortedRecords;
  protected Comparator<AbstractRecord> recordComparator;
  protected boolean sorted = false;

  public SortBase(Operator child, Comparator<AbstractRecord> recordComparator) {
    super(child);
    this.recordComparator = recordComparator;

  }
}
