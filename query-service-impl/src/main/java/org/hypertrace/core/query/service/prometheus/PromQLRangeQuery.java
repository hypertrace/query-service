package org.hypertrace.core.query.service.prometheus;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;

@Getter
@Builder
class PromQLRangeQuery {
  @NonNull @Singular private List<String> queries;

  @NonNull private Instant startTime;

  @NonNull private Instant endTime;

  @NonNull private Duration step;
}
