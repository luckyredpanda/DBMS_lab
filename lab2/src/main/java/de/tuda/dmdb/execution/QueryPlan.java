package de.tuda.dmdb.execution;

import de.tuda.dmdb.operator.Operator;

public abstract class QueryPlan {

  public abstract Operator compilePlan();
}
