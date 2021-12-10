package org.hypertrace.core.query.service.prometheus;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PromQLQueryTest {
  @Test
  public void testPromQLInstantQuery() {
    PromQLInstantQueries promQLQuery =
        PromQLInstantQueries.builder()
            .metricNameToQueryMap(Map.of("num_calls", "num_calls{tenantId=\"tenant1\"}"))
            .evalTime(Instant.ofEpochMilli(1637756020000L))
            .build();
    Assertions.assertEquals(1, promQLQuery.getMetricNameToQueryMap().size());
    Assertions.assertEquals(1637756020L, promQLQuery.getEvalTime().getEpochSecond());
    Assertions.assertEquals(
        "num_calls{tenantId=\"tenant1\"}", promQLQuery.getMetricNameToQueryMap().get(0));
  }

  @Test
  public void testPromQLRangeQuery() {
    PromQLRangeQueries promQLQuery =
        PromQLRangeQueries.builder()
            .metricNameToQueryMap(Map.of("num_calls", "num_calls{tenantId=\"tenant1\"}"))
            .startTime(Instant.ofEpochMilli(1637756010000L))
            .endTime(Instant.ofEpochMilli(1637756020000L))
            .period(Duration.of(15000, ChronoUnit.MILLIS))
            .build();
    Assertions.assertEquals(1, promQLQuery.getMetricNameToQueryMap().size());
    Assertions.assertEquals(1637756010L, promQLQuery.getStartTime().getEpochSecond());
    Assertions.assertEquals(1637756020L, promQLQuery.getEndTime().getEpochSecond());
    Assertions.assertEquals(15L, promQLQuery.getPeriod().getSeconds());
    Assertions.assertEquals(
        "num_calls{tenantId=\"tenant1\"}", promQLQuery.getMetricNameToQueryMap().get(0));
  }

  @Test
  public void testPromQLBadInstantQuery() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> {
          PromQLInstantQueries promQLQuery =
              PromQLInstantQueries.builder()
                  .metricNameToQueryMap(Map.of("num_calls", "num_calls{tenantId=\"tenant1\"}"))
                  .build();
        });
  }

  @Test
  public void testPromQLBadRangeQuery() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> {
          PromQLRangeQueries promQLQuery =
              PromQLRangeQueries.builder()
                  .startTime(Instant.ofEpochMilli(1637756010000L))
                  .period(Duration.of(15000, ChronoUnit.MILLIS))
                  .build();
        });
  }
}
