package de.tuda.dmdb.operator;

public abstract class BinaryOperator extends Operator {
  protected Operator leftChild;
  protected Operator rightChild;

  public BinaryOperator(Operator leftChild, Operator rightChild) {
    this.leftChild = leftChild;
    this.rightChild = rightChild;
  }

  public Operator getLeftChild() {
    return leftChild;
  }

  public void setLeftChild(Operator leftChild) {
    this.leftChild = leftChild;
  }

  public Operator getRightChild() {
    return rightChild;
  }

  public void setRightChild(Operator rightChild) {
    this.rightChild = rightChild;
  }
}
