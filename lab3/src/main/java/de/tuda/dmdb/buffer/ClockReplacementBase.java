package de.tuda.dmdb.buffer;

public abstract class ClockReplacementBase extends ReplacementPolicy {

  protected int clockHandPos = 0; // indicates current clock-hand position

  public ClockReplacementBase(int poolSize) {
    super(poolSize);
  }

  /**
   * Return the current clockHand position
   *
   * @return the clockHandPos
   */
  public Integer getClockHandPos() {
    return clockHandPos;
  }
}
