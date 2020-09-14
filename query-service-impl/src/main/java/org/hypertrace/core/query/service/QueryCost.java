package org.hypertrace.core.query.service;

public class QueryCost {

  /**
   * Return the cost to evaluate the request.
   *
   * @return -1 means it cannot handle the request else 0 (super fast) to 1 very expensive
   */
  double cost;
  /**
   * Allows the request handler to return additional context as part of RequestHandler.canHandle
   * method in RequestHandler. This will be passed in to the RequestHandler.handleRequest
   */
  Object context;

  public QueryCost(double cost) {
    this(cost, null);
  }

  public QueryCost(double cost, Object context) {
    this.cost = cost;
    this.context = context;
  }

  public double getCost() {
    return cost;
  }

  public void setCost(double cost) {
    this.cost = cost;
  }

  public Object getContext() {
    return context;
  }

  public void setContext(Object context) {
    this.context = context;
  }
}
