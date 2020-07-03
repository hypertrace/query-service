package org.hypertrace.core.query.service;

/**
 * Class to hold context for a query from the incoming request. We maintain a separate class for
 * QueryService so that the context for this service can evolve independent from the platform
 * RequestContext class.
 */
public class QueryContext {
  private final String tenantId;

  public QueryContext(String tenantId) {
    this.tenantId = tenantId;
  }

  public String getTenantId() {
    return tenantId;
  }
}
