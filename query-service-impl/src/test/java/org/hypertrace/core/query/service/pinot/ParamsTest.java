package org.hypertrace.core.query.service.pinot;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Params}
 */
public class ParamsTest {
  @Test
  public void testEquals() {
    Params params = Params.newBuilder().addStringParam("test").build();
    Assertions.assertEquals(params, params);
    Assertions.assertEquals(params, Params.newBuilder().addStringParam("test").build());
    Assertions.assertNotEquals(params, Params.newBuilder().addIntegerParam(1).build());

    params = Params.newBuilder().addStringParam("test1").addFloatParam(0.1f).build();
    Assertions.assertEquals(params, Params.newBuilder().addStringParam("test1").addFloatParam(0.1f).build());
  }
}
