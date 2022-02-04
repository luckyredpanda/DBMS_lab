package de.tuda.dmdb.operator;

import de.tuda.dmdb.storage.AbstractRecord;

/**
 * Implements a nested loop Join strategy
 *
 * @author melhindi
 */
public abstract class EquiJoinBase extends BinaryOperator {
  protected int leftAtt = 0; // index of the join column in the left child
  protected int rightAtt = 0; // index of the join column in the right child

  protected AbstractRecord leftRecord = null; // used to implement outer loop

  /**
   * Constructor for EquiJoinBase
   *
   * @param leftChild The left relation to use for the join computation
   * @param rightChild The right relation to use for the join computation
   * @param leftAtt The index of the join column in the left relation
   * @param rightAtt The index of the join column in the right relation
   */
  public EquiJoinBase(Operator leftChild, Operator rightChild, int leftAtt, int rightAtt) {
    super(leftChild, rightChild);
    this.leftAtt = leftAtt;
    this.rightAtt = rightAtt;
  }
}
