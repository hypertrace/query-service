package org.hypertrace.core.query.service.promql;

import java.util.List;
import java.util.Map;

public class MetricResult {
  private Map<String, String> metric;
  private List<MetricValue> value;

  public MetricResult(Map<String, String> metric, List<MetricValue> value) {
    this.metric = metric;
    this.value = value;
  }
}
