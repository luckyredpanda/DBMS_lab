package de.tuda.dmdb.operator;

import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import java.util.HashMap;

/**
 * Physical operator to compute a join using the Hash Join strategy. You can assume that the left
 * child is the smaller table and has unique keys in the join column
 *
 * @author melhindi
 */
public abstract class HashJoinBase extends BinaryOperator {
  protected int leftAtt = 0; // joint attribute in the left relation
  protected int rightAtt = 0; // joint attribute in the right relation
  protected HashMap<AbstractSQLValue, AbstractRecord>
      hashMap; // used to build hashmap on smaller relation

  /**
   * Physical operator to compute a join using the Hash Join strategy. You can assume that the left
   * child is the smaller table and has unique keys in the join column
   *
   * @param leftChild The left input relation of the join, you can assume it is the smaller one and
   *     is used to build the hash table
   * @param rightChild The right input relation of the join
   * @param leftAtt The index of the join attribute in the left relation
   * @param rightAtt The index of the join attribute in the right relation
   */
  public HashJoinBase(Operator leftChild, Operator rightChild, int leftAtt, int rightAtt) {
    super(leftChild, rightChild);
    this.leftAtt = leftAtt;
    this.rightAtt = rightAtt;
  }
}
