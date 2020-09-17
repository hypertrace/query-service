package org.hypertrace.core.query.service;

public class QueryCost {

  /**
   * Return the cost to evaluate the request.
   *
   * @return -1 means it cannot handle the request else 0 (super fast) to 1 very expensive
   */
  double cost;

  public QueryCost(double cost) {
    this.cost = cost;
  }

  public double getCost() {
    return cost;
  }

  public void setCost(double cost) {
    this.cost = cost;
  }
}
