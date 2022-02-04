package de.tuda.dmdb.buffer;

public abstract class LRUReplacementBase extends ReplacementPolicy {

  public LRUReplacementBase(int poolSize) {
    super(poolSize);
  }
}
