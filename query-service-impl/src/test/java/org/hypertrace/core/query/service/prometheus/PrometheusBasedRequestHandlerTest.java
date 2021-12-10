package org.hypertrace.core.query.service.prometheus;

import java.io.IOException;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PrometheusBasedRequestHandlerTest {

  private static MockWebServer mockWebServer;
  private static PrometheusBasedRequestHandler prometheusBasedRequestHandler;

  @BeforeEach
  public static void setUp() throws IOException {
    mockWebServer = new MockWebServer();
    mockWebServer.start(9090);
    //prometheusBasedRequestHandler = new PrometheusBasedRequestHandler(PrometheusTestUtils.getDefaultPrometheusConfig());
  }

  @Test
  public void test() {

  }
}
