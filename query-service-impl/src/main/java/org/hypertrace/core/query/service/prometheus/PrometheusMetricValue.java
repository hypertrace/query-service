package org.hypertrace.core.query.service.prometheus;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class PrometheusMetricValue {
  private long timeStamp;
  private double value;
}
