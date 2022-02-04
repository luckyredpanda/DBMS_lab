package de.tuda.dmdb.access;

public class RecordIdentifier {
  private int pageNumber = 0;
  private int slotNumber = 0;

  public RecordIdentifier(int pageNumber, int slotNumber) {
    this.pageNumber = pageNumber;
    this.slotNumber = slotNumber;
  }

  public int getPageNumber() {
    return pageNumber;
  }

  public void setPageNumber(int pageNumber) {
    this.pageNumber = pageNumber;
  }

  public int getSlotNumber() {
    return slotNumber;
  }

  public void setSlotNumber(int slotNumber) {
    this.slotNumber = slotNumber;
  }

  @Override
  public String toString() {
    return "(page:" + pageNumber + ", slot:" + slotNumber + ")";
  }
}
