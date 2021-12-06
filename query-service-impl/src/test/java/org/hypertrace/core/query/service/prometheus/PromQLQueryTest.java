package org.hypertrace.core.query.service.prometheus;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PromQLQueryTest {
  @Test
  public void testPromQLInstantQuery() {
    PromQLInstantQuery promQLQuery =
        PromQLInstantQuery.builder()
            .query("num_calls{tenantId=\"tenant1\"}")
            .evalTime(Instant.ofEpochMilli(1637756020000L))
            .build();
    Assertions.assertEquals(1, promQLQuery.getQueries().size());
    Assertions.assertEquals(1637756020L, promQLQuery.getEvalTime().getEpochSecond());
    Assertions.assertEquals("num_calls{tenantId=\"tenant1\"}", promQLQuery.getQueries().get(0));
  }

  @Test
  public void testPromQLRangeQuery() {
    PromQLRangeQuery promQLQuery =
        PromQLRangeQuery.builder()
            .query("num_calls{tenantId=\"tenant1\"}")
            .startTime(Instant.ofEpochMilli(1637756010000L))
            .endTime(Instant.ofEpochMilli(1637756020000L))
            .step(Duration.of(15000, ChronoUnit.MILLIS))
            .build();
    Assertions.assertEquals(1, promQLQuery.getQueries().size());
    Assertions.assertEquals(1637756010L, promQLQuery.getStartTime().getEpochSecond());
    Assertions.assertEquals(1637756020L, promQLQuery.getEndTime().getEpochSecond());
    Assertions.assertEquals(15L, promQLQuery.getStep().getSeconds());
    Assertions.assertEquals("num_calls{tenantId=\"tenant1\"}", promQLQuery.getQueries().get(0));
  }

  @Test
  public void testPromQLBadInstantQuery() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> {
          PromQLInstantQuery promQLQuery =
              PromQLInstantQuery.builder().query("num_calls{tenantId=\"tenant1\"}").build();
        });
  }

  @Test
  public void testPromQLBadRangeQuery() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> {
          PromQLRangeQuery promQLQuery =
              PromQLRangeQuery.builder()
                  .startTime(Instant.ofEpochMilli(1637756010000L))
                  .step(Duration.of(15000, ChronoUnit.MILLIS))
                  .build();
        });
  }
}
