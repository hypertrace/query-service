package org.hypertrace.core.query.service;

import java.time.Duration;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class QueryTimeRange {

  Instant startTime;
  Instant endTime;
  Duration duration;
}
