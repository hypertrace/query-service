package org.hypertrace.core.query.service.prometheus;

import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;

@Getter
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@EqualsAndHashCode
public class PrometheusMetricResult {
  @NonNull @Singular private Map<String, String> metricAttributes;
  @NonNull @Singular private List<PrometheusMetricValue> values;
}
