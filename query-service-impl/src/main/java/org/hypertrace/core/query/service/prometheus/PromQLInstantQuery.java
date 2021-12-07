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

  @NonNull private Instant evalTime;
}
