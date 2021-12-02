package org.hypertrace.core.query.service.prometheus;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class PrometheusMetricValue {
  private long timeStamp;
  private double value;
}
