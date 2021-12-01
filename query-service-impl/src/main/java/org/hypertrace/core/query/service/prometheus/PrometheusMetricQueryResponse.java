package org.hypertrace.core.query.service.prometheus;

import java.util.List;

// todo: PrometheusMetricQueryResponse
public class PrometheusMetricQueryResponse {
  private String status;
  private String resultType;
  private List<PrometheusMetricResult> metrics; // PromQLMetric, PrometheusMetricResponse, ?

  public PrometheusMetricQueryResponse(
      String status, String resultType, List<PrometheusMetricResult> metrics) {
    this.status = status;
    this.resultType = resultType;
    this.metrics = metrics;
  }
}
