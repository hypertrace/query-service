package org.hypertrace.core.query.service.prometheus;

import java.util.List;

public class PromqlQuery {

  private final List<String> queries;
  private final boolean isInstantRequest;
  private final long startTimeMs;
  private final long endTimeMs;
  private final long stepMs;

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

  public List<String> getQueries() {
    return queries;
  }
}
