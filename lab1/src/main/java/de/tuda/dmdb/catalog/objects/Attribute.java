package de.tuda.dmdb.catalog.objects;

public class Attribute {
  private String name = "";
  private Table table;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name.toUpperCase();
  }

  public Table getTable() {
    return table;
  }

  public void setTable(Table table) {
    this.table = table;
  }

  @Override
  public boolean equals(Object o) {
    Attribute att = (Attribute) o;
    if (table != null && !this.table.equals(att.table)) return false;

    if (!this.name.equals(att.name)) return false;

    return true;
  }
}
