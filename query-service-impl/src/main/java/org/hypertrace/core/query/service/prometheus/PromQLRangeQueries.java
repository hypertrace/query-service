package org.hypertrace.core.query.service.prometheus;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
class PromQLRangeQueries {
  @NonNull @Singular private List<String> queries;

  @NonNull private Instant startTime;

  @NonNull private Instant endTime;

  /*
   * It refers to the step query param argument of PromQL range query Rest API.
   * https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries
   * */
  @NonNull private Duration period;
}
