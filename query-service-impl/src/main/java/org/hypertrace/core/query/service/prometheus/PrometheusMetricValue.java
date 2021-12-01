package org.hypertrace.core.query.service.prometheus;

public class PrometheusMetricValue {
  private long timeStamp;
  private double value;

  public PrometheusMetricValue(long timeStamp, double value) {
    this.timeStamp = timeStamp;
    this.value = value;
  }
}
