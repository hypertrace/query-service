package org.hypertrace.core.query.service.prometheus;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import lombok.NonNull;
import lombok.Singular;

public class PromQLRangeQuery {
  @NonNull @Singular private List<String> queries;

  @NonNull private Instant startTime;

  @NonNull private Instant endTime;

  @NonNull private Duration step;
}
