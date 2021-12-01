package org.hypertrace.core.query.service.prometheus;

import java.util.List;
import java.util.Map;

public class PrometheusMetricResult {
  private Map<String, String> metric;
  private List<PrometheusMetricValue> values;

  public PrometheusMetricResult(Map<String, String> metric, List<PrometheusMetricValue> values) {
    this.metric = metric;
    this.values = values;
  }
}
