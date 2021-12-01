package org.hypertrace.core.query.service.prometheus;

import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

@Getter
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class PromQLQuery {
  @Singular private List<String> queries;
  private long evalTimeMs; // we are removing this
  private boolean isInstantRequest;
  private long startTimeMs; // Instant
  private long endTimeMs; // Instant
  private long stepMs; // Duration
}
