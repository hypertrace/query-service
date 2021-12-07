package org.hypertrace.core.query.service;

import java.time.Duration;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class QueryTimeRange {

  private final Instant startTime;
  private final Instant endTime;
  private final Duration duration;
}
