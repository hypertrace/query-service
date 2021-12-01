package org.hypertrace.core.query.service.prometheus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PromQLQueryTest {
  @Test
  public void testPromQLQuery() {
    PromQLQuery promQLQuery =
        PromQLQuery.builder()
            .query("num_calls{tenantId=\"tenant1\"}")
            .evalTimeMs(1637756020000L)
            .isInstantRequest(true)
            .build();
    Assertions.assertEquals("num_calls{tenantId=\"tenant1\"}", promQLQuery.getQueries().get(0));
  }
}
