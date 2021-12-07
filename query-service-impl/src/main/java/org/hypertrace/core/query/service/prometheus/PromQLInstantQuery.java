package org.hypertrace.core.query.service.prometheus;

import java.time.Instant;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;

@Getter
@Builder
class PromQLInstantQuery {
  @NonNull @Singular private List<String> queries;

  /*
   * Eval Time is the time of instant when the metrics queries are evaluated.
   * It is one of query param argument of PromQL Instant query API
   * https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries
   *
   * In the Query Service context, for a given time range filter, it is an higher
   * range of the time filter.
   * e.g start_time_millis >= lower AND start_time_millis < higher
   * */
  @NonNull private Instant evalTime;
}
