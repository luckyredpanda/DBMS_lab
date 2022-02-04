package de.tuda.dmdb.operator;

import java.util.List;

public abstract class ProjectionBase extends UnaryOperator {
  protected List<Integer> attributes;

  protected ProjectionBase(Operator child, List<Integer> attributes) {
    super(child);

    this.attributes = attributes;
  }

  /**
   * Get the list of attributes used for the projection
   *
   * @return the list of attributes as vector
   */
  public List<Integer> getAttributes() {
    return attributes;
  }

  /**
   * Set the list of attributes used for the projection
   *
   * @param attributes the list of attributes as vector to set
   */
  public void setAttributes(List<Integer> attributes) {
    this.attributes = attributes;
  }
}
