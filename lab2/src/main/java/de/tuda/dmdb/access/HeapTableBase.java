package de.tuda.dmdb.access;

import de.tuda.dmdb.buffer.BufferManagerBase;
import de.tuda.dmdb.buffer.exercise.BufferManager;
import de.tuda.dmdb.storage.AbstractPage;
import de.tuda.dmdb.storage.AbstractRecord;
import java.util.Vector;

public abstract class HeapTableBase extends AbstractTable {

  // Use BufferManager to get AbstractPages from pin()
  protected BufferManagerBase bufferManager = BufferManager.getInstance();

  // metadata
  protected Vector<Integer> pageNumbers = new Vector<Integer>();

  // data
  protected int lastPageNumber;

  /**
   * Constructs table from record prototype
   *
   * @param prototypeRecord the prototype of records in this table
   */
  public HeapTableBase(AbstractRecord prototypeRecord) {
    super(prototypeRecord);

    // create first empty page
    AbstractPage lastPage =
        BufferManager.getInstance().createDefaultPage(this.prototype.getFixedLength());
    this.lastPageNumber = lastPage.getPageNumber();
    this.addPage(lastPage.getPageNumber());
  }

  /**
   * Adds a new page to container and creates unique page number
   *
   * @param pageNumber Page number
   */
  protected void addPage(int pageNumber) {
    this.pageNumbers.add(pageNumber);
  }

  protected AbstractPage getPage(int pageNumber) {
    return bufferManager.pin(pageNumber);
  }

  @Override
  public Integer getPageNumber(int index) {
    return pageNumbers.get(index);
  }

  @Override
  public int getNumPages() {
    return pageNumbers.size();
  }

  @Override
  public AbstractRecord lookup(RecordIdentifier rid) {
    return this.lookup(rid.getPageNumber(), rid.getSlotNumber());
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    for (Integer pageNum : pageNumbers) {
      stringBuilder.append(pageNum);
    }
    return stringBuilder.toString();
  }
}
