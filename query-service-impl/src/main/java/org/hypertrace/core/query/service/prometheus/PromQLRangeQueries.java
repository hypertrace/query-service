package org.hypertrace.core.query.service.prometheus;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
class PromQLRangeQueries {
  @NonNull Map<String, String> metricNameToQueryMap;

  @NonNull Instant startTime;

  @NonNull Instant endTime;

  /*
   * It refers to the step query param argument of PromQL range query Rest API.
   * https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries
   * */
  @NonNull Duration period;
}
