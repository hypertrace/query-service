package org.hypertrace.core.query.service.promql;

public class MetricValue {
  private long timeStamp;
  private double value;

  public MetricValue(long timeStamp, double value) {
    this.timeStamp = timeStamp;
    this.value = value;
  }
}
