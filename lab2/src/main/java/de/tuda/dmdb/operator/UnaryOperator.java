package de.tuda.dmdb.operator;

public abstract class UnaryOperator extends Operator {
  protected Operator child;

  public UnaryOperator(Operator child) {
    this.child = child;
  }

  public Operator getChild() {
    return child;
  }

  public void setChild(Operator child) {
    this.child = child;
  }
}
