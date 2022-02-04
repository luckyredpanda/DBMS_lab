package de.tuda.dmdb.operator.exercise;

import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.access.RecordIdentifier;
import de.tuda.dmdb.buffer.BufferManagerBase;
import de.tuda.dmdb.buffer.exercise.BufferManager;
import de.tuda.dmdb.operator.TableScanBase;
import de.tuda.dmdb.storage.AbstractPage;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.exercise.RowPage;

public class TableScan extends TableScanBase {

  // Use BufferManager to get AbstractPages
  protected BufferManagerBase bufferManager = BufferManager.getInstance();

  public TableScan(AbstractTable table) {
    super(table);
  }
  int page_nums = 0;
  int slot_num = 0;
  int open_i =0;
  int count = 0;

  @Override
  public void open() {
    // TODO implement this method
    // Reset internal state and prepare to deliver first tuple
    open_i = 1;        // use open_i to inform if the open() function is called.
    if (table.getRecordCount()>0) {
      for (int i=0;i<table.getRecordCount();i++){
        RecordIdentifier rid = table.getRecordIDFromRowId(i);
        AbstractRecord r = table.lookup(rid);
        int page = rid.getPageNumber();
        int slot = rid.getSlotNumber();
        AbstractPage p = null;
        if (bufferManager.getPageTable().get(page) == null) {
          p = new RowPage(table.getPrototype().getFixedLength());
          p.setPageNumber(page);
          p.insert(slot,r,true);   //insert record to buffer manager
        }
        else {
          p = bufferManager.getPageTable().get(page);
          p.insert(slot,r,true);
        }
        bufferManager.getPageTable().put(page,p);
      }
      page_nums = this.table.getRecordIDFromRowId(0).getPageNumber();  //get the first tuple from table
      slot_num = this.table.getRecordIDFromRowId(0).getSlotNumber();
    }
    else {
      AbstractPage p = new RowPage(table.getPrototype().getFixedLength());
      bufferManager.getPageTable().put(0,p);
    }
  }

  @Override
  public AbstractRecord next() {
    // TODO implement this method
    // Use this.table.getPrototype().clone() to get a Record with the correct schema
    if (open_i == 0) {
      NullPointerException e = new NullPointerException();
      throw e;
    }
    else {
      if (count<this.table.getRecordCount() && this.table.getRecordCount()>0 ) {
        if (bufferManager.getPageTable().get(page_nums).getNumRecords() == 0) return null;
        AbstractRecord r = this.table.getPrototype().clone();
        AbstractPage p = this.bufferManager.getPageTable().get(page_nums);
        p.read(slot_num, r);    //read record from buffer manager
        if (slot_num == p.getNumRecords()) bufferManager.getPageTable().remove(page_nums);  // if all slots have been read, unpin the page.
        count++;
        if (count<this.table.getRecordCount()) {
          RecordIdentifier rid = table.getRecordIDFromRowId(count);
          page_nums = rid.getPageNumber();
          slot_num = rid.getSlotNumber();
        }
        return r;    //return record
      }
      return null;  //if there is no record left in the HashTable, return null;
    }
  }

  @Override
  public void close() {
    // TODO implement this method
    this.bufferManager.getPageTable().clear();   //clear PageTable and reset all variables
    page_nums = 0;
    slot_num = 0;
    count = 0;
  }
}
