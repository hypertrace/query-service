package org.hypertrace.core.query.service.prometheus;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PromQLQueryTest {
  @Test
  public void testPromQLInstantQuery() {
    PromQLQuery promQLQuery =
        PromQLQuery.builder()
            .query("num_calls{tenantId=\"tenant1\"}")
            .endTime(Instant.ofEpochMilli(1637756020000L))
            .isInstantRequest(true)
            .build();
    Assertions.assertTrue(promQLQuery.isInstantRequest());
    Assertions.assertEquals(1, promQLQuery.getQueries().size());
    Assertions.assertEquals(1637756020L, promQLQuery.getEndTime().getEpochSecond());
    Assertions.assertEquals("num_calls{tenantId=\"tenant1\"}", promQLQuery.getQueries().get(0));
  }

  @Test
  public void testPromQLRangeQuery() {
    PromQLQuery promQLQuery =
        PromQLQuery.builder()
            .query("num_calls{tenantId=\"tenant1\"}")
            .startTime(Instant.ofEpochMilli(1637756010000L))
            .endTime(Instant.ofEpochMilli(1637756020000L))
            .isInstantRequest(false)
            .step(Duration.of(15000, ChronoUnit.MILLIS))
            .build();
    Assertions.assertFalse(promQLQuery.isInstantRequest());
    Assertions.assertEquals(1, promQLQuery.getQueries().size());
    Assertions.assertEquals(1637756010L, promQLQuery.getStartTime().getEpochSecond());
    Assertions.assertEquals(1637756020L, promQLQuery.getEndTime().getEpochSecond());
    Assertions.assertEquals(15L, promQLQuery.getStep().getSeconds());
    Assertions.assertEquals("num_calls{tenantId=\"tenant1\"}", promQLQuery.getQueries().get(0));
  }

  @Test
  public void testPromQLBadInstantQuery() {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> {
          PromQLQuery promQLQuery =
              PromQLQuery.builder()
                  .query("num_calls{tenantId=\"tenant1\"}")
                  .startTime(Instant.ofEpochMilli(1637756010000L))
                  .isInstantRequest(true)
                  .build();
        });
  }

  @Test
  public void testPromQLBadRangeQuery() {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> {
          PromQLQuery promQLQuery =
              PromQLQuery.builder()
                  .query("num_calls{tenantId=\"tenant1\"}")
                  .startTime(Instant.ofEpochMilli(1637756010000L))
                  .isInstantRequest(false)
                  .step(Duration.of(15000, ChronoUnit.MILLIS))
                  .build();
        });
  }

  @Test
  public void testPromQLBadQuery() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> {
          PromQLQuery promQLQuery =
              PromQLQuery.builder()
                  .endTime(Instant.ofEpochMilli(1637756010000L))
                  .isInstantRequest(true)
                  .build();
        });
  }

  @Test
  public void testPromQLBadQueryWithSizeZero() {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> {
          PromQLQuery promQLQuery =
              PromQLQuery.builder()
                  .queries(List.of())
                  .endTime(Instant.ofEpochMilli(1637756010000L))
                  .isInstantRequest(true)
                  .build();
        });
  }
}
