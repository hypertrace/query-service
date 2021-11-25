package org.hypertrace.core.query.service.promql;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PromQLRestClientTest {

  @Test
  public void testPromQLQuery() {
    PromQLQuery promQLQuery =
        PromQLQuery.Builder.newBuilder()
            .addQuery("num_calls{tenantId=\"tenant1\"}")
            .setEvalTimeMs(1637756020000L)
            .setInstantRequest(true)
            .build();
    Assertions.assertEquals("num_calls{tenantId=\"tenant1\"}", promQLQuery.getQuery().get(0));
  }
}
