package org.hypertrace.core.query.service.promql;

import java.util.List;

public class MetricResponse {
  private String status;
  private String resultType;
  private List<MetricResult> metrics;

  public MetricResponse(String status, String resultType, List<MetricResult> metrics) {
    this.status = status;
    this.resultType = resultType;
    this.metrics = metrics;
  }
}
