package org.hypertrace.core.query.service.prometheus;

import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;

@Getter
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class PrometheusMetricQueryResponse {
  @NonNull private String status;
  private String resultType;
  @Singular private List<PrometheusMetricResult> metrics;
}
