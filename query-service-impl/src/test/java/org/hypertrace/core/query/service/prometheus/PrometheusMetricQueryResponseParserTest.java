package org.hypertrace.core.query.service.prometheus;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PrometheusMetricQueryResponseParserTest {
  @Test
  public void testVectorResponse() throws IOException {
    URL fileUrl =
        PrometheusMetricQueryResponseParserTest.class
            .getClassLoader()
            .getResource("promql_error_count_vector.json");
    String content = new String(Files.readAllBytes(Paths.get(fileUrl.getFile())));

    PrometheusMetricQueryResponse response = PrometheusMetricQueryResponseParser.parse(content);

    Assertions.assertEquals("success", response.getStatus());
    Assertions.assertEquals("vector", response.getResultType());
    Assertions.assertEquals(2, response.getMetrics().size());

    // test first metric
    PrometheusMetricResult firstMetricResult =
        PrometheusMetricResult.builder()
            .metricAttribute("__name__", "errorCount")
            .metricAttribute("job", "prometheus")
            .metricAttribute("instance", "localhost:9090")
            .value(new PrometheusMetricValue(1435781451000L, 1.0))
            .build();

    Assertions.assertTrue(response.getMetrics().contains(firstMetricResult));

    // test second metric
    PrometheusMetricResult secondMetricResult =
        PrometheusMetricResult.builder()
            .metricAttribute("__name__", "errorCount")
            .metricAttribute("job", "node")
            .metricAttribute("instance", "localhost:9100")
            .value(new PrometheusMetricValue(1435781451000L, 0.0))
            .build();

    Assertions.assertTrue(response.getMetrics().contains(secondMetricResult));
  }

  @Test
  public void testMatrixResponse() throws IOException {
    URL fileUrl =
        PrometheusMetricQueryResponseParserTest.class
            .getClassLoader()
            .getResource("promql_error_count_matrix.json");
    String content = new String(Files.readAllBytes(Paths.get(fileUrl.getFile())));

    PrometheusMetricQueryResponse response = PrometheusMetricQueryResponseParser.parse(content);

    Assertions.assertEquals("success", response.getStatus());
    Assertions.assertEquals("matrix", response.getResultType());
    Assertions.assertEquals(2, response.getMetrics().size());

    // test first metric
    PrometheusMetricResult firstMetricResult =
        PrometheusMetricResult.builder()
            .metricAttribute("__name__", "errorCount")
            .metricAttribute("job", "prometheus")
            .metricAttribute("instance", "localhost:9090")
            .value(new PrometheusMetricValue(1435781430000L, 1.0))
            .value(new PrometheusMetricValue(1435781445000L, 2.0))
            .value(new PrometheusMetricValue(1435781460000L, 3.0))
            .build();

    Assertions.assertTrue(response.getMetrics().contains(firstMetricResult));

    // test second metric
    PrometheusMetricResult secondMetricResult =
        PrometheusMetricResult.builder()
            .metricAttribute("__name__", "errorCount")
            .metricAttribute("job", "node")
            .metricAttribute("instance", "localhost:9091")
            .value(new PrometheusMetricValue(1435781430000L, 0.0))
            .value(new PrometheusMetricValue(1435781445000L, 0.0))
            .value(new PrometheusMetricValue(1435781460000L, 1.0))
            .build();

    Assertions.assertTrue(response.getMetrics().contains(secondMetricResult));
  }

  @Test
  public void testErrorResponse() throws IOException {
    URL fileUrl =
        PrometheusMetricQueryResponseParserTest.class
            .getClassLoader()
            .getResource("promql_storage_vector_failure.json");
    String content = new String(Files.readAllBytes(Paths.get(fileUrl.getFile())));

    PrometheusMetricQueryResponse response = PrometheusMetricQueryResponseParser.parse(content);

    Assertions.assertEquals("error", response.getStatus());
    Assertions.assertNull(response.getResultType());
    Assertions.assertEquals(0, response.getMetrics().size());
  }
}
