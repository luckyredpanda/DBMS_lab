package de.tuda.dmdb.catalog.objects;

public class Table extends DatabaseObject {

  public Table(String name) {
    super(name);
  }

  @Override
  public boolean equals(Object o) {
    Table tab = (Table) o;

    if (!this.name.equals(tab.name)) return false;

    return true;
  }
}
