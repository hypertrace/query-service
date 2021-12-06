package org.hypertrace.core.query.service.prometheus;

import java.time.Instant;
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
public class PromQLInstantQuery {
  @NonNull @Singular private List<String> queries;

  @NonNull private Instant evalTime;
}
