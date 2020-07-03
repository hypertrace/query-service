package org.hypertrace.core.query.service.pinot;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PinotUtilsTest {

  @Test
  public void testZkPath() {
    Assertions.assertEquals(
        "localhost:2181/pinot", PinotUtils.getZkPath("localhost:2181", "pinot"));
    Assertions.assertEquals(
        "localhost:2181/pinot", PinotUtils.getZkPath("localhost:2181/", "pinot"));
    Assertions.assertEquals(
        "localhost:2181/pinot/myView", PinotUtils.getZkPath("localhost:2181/pinot", "myView"));
    Assertions.assertEquals(
        "localhost:2181/pinot/myView", PinotUtils.getZkPath("localhost:2181/pinot/", "myView"));
  }
}
