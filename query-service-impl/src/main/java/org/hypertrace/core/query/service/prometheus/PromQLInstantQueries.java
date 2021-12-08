package org.hypertrace.core.query.service.prometheus;

import java.time.Instant;
import java.util.List;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
class PromQLInstantQueries {
  @NonNull @Singular List<String> queries;

  /*
   * Eval Time is the time of instant when the metrics queries are evaluated.
   * It is one of the query param arguments of PromQL Instant query API.
   * https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries
   *
   * In the Query Service context, for a given time range filter, it is a higher
   * range of the time filter.
   * e.g start_time_millis >= lower AND start_time_millis < higher
   * */
  @NonNull Instant evalTime;
}
