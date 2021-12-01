package org.hypertrace.core.query.service.prometheus;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;

@Getter
@Builder(builderClassName = "Builder", buildMethodName = "build")
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class PromQLQuery {

  @NonNull @Singular private List<String> queries;
  private boolean isInstantRequest;
  private Instant startTime;
  private Instant endTime;
  private Duration step;

  static class Builder {
    PromQLQuery build() {
      if (queries.size() == 0) {
        throw new UnsupportedOperationException(
            "Minimum one query requires either for Instant or Range query");
      }
      if (isInstantRequest && endTime == null) {
        throw new UnsupportedOperationException("endTime requires for instant query");
      } else if (!isInstantRequest && (startTime == null || endTime == null || step == null)) {
        throw new UnsupportedOperationException(
            "startTime, endTime and step requires for range query");
      }
      return new PromQLQuery(queries, isInstantRequest, startTime, endTime, step);
    }
  }
}
