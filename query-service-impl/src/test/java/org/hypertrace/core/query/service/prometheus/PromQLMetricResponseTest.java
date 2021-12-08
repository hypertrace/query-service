package org.hypertrace.core.query.service.prometheus;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import org.hypertrace.core.query.service.prometheus.PromQLMetricResponse.PromQLMetricResult;
import org.hypertrace.core.query.service.prometheus.PromQLMetricResponse.PromQLMetricValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PromQLMetricResponseTest {

  @Test
  public void testVectorResponse() throws IOException {
    URL fileUrl =
        PromQLMetricResponseTest.class
            .getClassLoader()
            .getResource("promql_error_count_vector.json");
    String content = new String(Files.readAllBytes(Paths.get(fileUrl.getFile())));

    PromQLMetricResponse response = PromQLMetricResponse.fromJson(content);

    Assertions.assertEquals("success", response.getStatus());
    Assertions.assertEquals("vector", response.getData().getResultType());
    Assertions.assertEquals(2, response.getData().getResult().size());

    // test first metric
    PromQLMetricResult firstMetricResult =
        PromQLMetricResult.builder()
            .metricAttribute("__name__", "errorCount")
            .metricAttribute("job", "prometheus")
            .metricAttribute("instance", "localhost:9090")
            .value(new PromQLMetricValue(Instant.ofEpochMilli(1435781451000L), 1.0))
            .build();

    Assertions.assertTrue(response.getData().getResult().contains(firstMetricResult));

    // test second metric
    PromQLMetricResult secondMetricResult =
        PromQLMetricResult.builder()
            .metricAttribute("__name__", "errorCount")
            .metricAttribute("job", "node")
            .metricAttribute("instance", "localhost:9100")
            .value(new PromQLMetricValue(Instant.ofEpochMilli(1435781451000L), 0.0))
            .build();

    Assertions.assertTrue(response.getData().getResult().contains(secondMetricResult));
  }

  @Test
  public void testMatrixResponse() throws IOException {
    URL fileUrl =
        PromQLMetricResponseTest.class
            .getClassLoader()
            .getResource("promql_error_count_matrix.json");
    String content = new String(Files.readAllBytes(Paths.get(fileUrl.getFile())));

    PromQLMetricResponse response = PromQLMetricResponse.fromJson(content);

    Assertions.assertEquals("success", response.getStatus());
    Assertions.assertEquals("matrix", response.getData().getResultType());
    Assertions.assertEquals(2, response.getData().getResult().size());

    // test first metric
    PromQLMetricResult firstMetricResult =
        PromQLMetricResult.builder()
            .metricAttribute("__name__", "errorCount")
            .metricAttribute("job", "prometheus")
            .metricAttribute("instance", "localhost:9090")
            .value(new PromQLMetricValue(Instant.ofEpochMilli(1435781430000L), 1.0))
            .value(new PromQLMetricValue(Instant.ofEpochMilli(1435781445000L), 2.0))
            .value(new PromQLMetricValue(Instant.ofEpochMilli(1435781460000L), 3.0))
            .build();

    Assertions.assertTrue(response.getData().getResult().contains(firstMetricResult));

    // test second metric
    PromQLMetricResult secondMetricResult =
        PromQLMetricResult.builder()
            .metricAttribute("__name__", "errorCount")
            .metricAttribute("job", "node")
            .metricAttribute("instance", "localhost:9091")
            .value(new PromQLMetricValue(Instant.ofEpochMilli(1435781430000L), 0.0))
            .value(new PromQLMetricValue(Instant.ofEpochMilli(1435781445000L), 0.0))
            .value(new PromQLMetricValue(Instant.ofEpochMilli(1435781460000L), 1.0))
            .build();

    Assertions.assertTrue(response.getData().getResult().contains(secondMetricResult));
  }

  @Test
  public void testErrorResponse() throws IOException {
    URL fileUrl =
        PromQLMetricResponseTest.class
            .getClassLoader()
            .getResource("promql_storage_vector_failure.json");
    String content = new String(Files.readAllBytes(Paths.get(fileUrl.getFile())));

    PromQLMetricResponse response = PromQLMetricResponse.fromJson(content);

    Assertions.assertEquals("error", response.getStatus());
    Assertions.assertNull(response.getData());
  }
}
