package org.hypertrace.core.query.service;

import java.time.Duration;

public class QueryTimeRange {
  private final long startTime;
  private final long endTime;
  private final Duration duration;

  public QueryTimeRange(long startTime, long endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.duration = Duration.ofMillis(startTime - endTime);
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public Duration getDuration() {
    return duration;
  }
}
