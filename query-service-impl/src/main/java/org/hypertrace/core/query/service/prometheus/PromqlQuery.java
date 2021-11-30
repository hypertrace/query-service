package org.hypertrace.core.query.service.prometheus;

import java.util.List;

public class PromqlQuery {

  private List<String> queries;
  private boolean isInstantRequest;
  private long startTimeMs;
  private long endTimeMs;
  private long stepMs;

  public PromqlQuery(
      List<String> queries,
      boolean isInstantRequest,
      long startTimeMs,
      long endTimeMs,
      long stepMs) {
    this.queries = queries;
    this.isInstantRequest = isInstantRequest;
    this.startTimeMs = startTimeMs;
    this.endTimeMs = endTimeMs;
    this.stepMs = stepMs;
  }
}
