package org.hypertrace.core.query.service.prometheus;

import java.util.List;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class PromQLQuery {
  private List<String> queries;
  private long evalTimeMs; // we are removing this
  private boolean isInstantRequest;
  private long startTimeMs; // Instant
  private long endTimeMs; // Instant
  private long stepMs; // Duration
}
