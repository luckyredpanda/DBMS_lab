package de.tuda.dmdb.catalog.objects;

public class Index extends DatabaseObject {
  private IndexType type = IndexType.BPlusTree;

  public Index(String name) {
    super(name);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name.toUpperCase();
  }

  public IndexType getType() {
    return type;
  }

  public void setType(IndexType type) {
    this.type = type;
  }
}
