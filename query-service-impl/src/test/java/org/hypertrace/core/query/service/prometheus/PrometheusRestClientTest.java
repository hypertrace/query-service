package org.hypertrace.core.query.service.prometheus;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class PrometheusRestClientTest {

  private static MockWebServer mockWebServer;
  private OkHttpClient okHttpClient = new OkHttpClient();
  private PrometheusRestClient prometheusRestClient = new PrometheusRestClient("localhost", 9090);

  @BeforeAll
  public static void setUp() throws IOException {
    mockWebServer = new MockWebServer();
    mockWebServer.start(9090);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    mockWebServer.shutdown();
  }

  @Test
  public void testSingleInstantQuery() throws IOException {
    mockWebServer.enqueue(getSuccessMockResponse("promql_error_count_vector.json"));

    PromQLQuery query =
        PromQLQuery.builder()
            .query("errorCount")
            .endTime(Instant.ofEpochMilli(1435781451000L))
            .isInstantRequest(true)
            .build();

    Map<Request, PromQLMetricResponse> metricQueryResponses = prometheusRestClient.execute(query);

    Assertions.assertEquals(1, metricQueryResponses.size());
  }

  @Test
  public void testSingleRangeQuery() throws IOException {
    mockWebServer.enqueue(getSuccessMockResponse("promql_error_count_matrix.json"));

    PromQLQuery query =
        PromQLQuery.builder()
            .query("errorCount")
            .startTime(Instant.ofEpochMilli(1435781430000L))
            .endTime(Instant.ofEpochMilli(1435781460000L))
            .isInstantRequest(false)
            .step(Duration.of(15000, ChronoUnit.MILLIS))
            .build();

    Map<Request, PromQLMetricResponse> metricQueryResponses = prometheusRestClient.execute(query);

    Assertions.assertEquals(1, metricQueryResponses.size());
  }

  @Test
  public void testMultipleInstantQuery() throws IOException {
    List<String> files = List.of("promql_error_count_vector.json", "promql_num_call_vector.json");
    files.stream().forEach(fileName -> mockWebServer.enqueue(getSuccessMockResponse(fileName)));

    PromQLQuery query =
        PromQLQuery.builder()
            .query("errorCount")
            .query("numCall")
            .endTime(Instant.ofEpochMilli(1435781451000L))
            .isInstantRequest(true)
            .build();

    Map<Request, PromQLMetricResponse> metricQueryResponses = prometheusRestClient.execute(query);

    Assertions.assertEquals(2, metricQueryResponses.size());
  }

  @Test
  public void testMultipleRangeQuery() throws IOException {
    List<String> files = List.of("promql_error_count_matrix.json", "promql_num_call_matrix.json");
    files.stream().forEach(fileName -> mockWebServer.enqueue(getSuccessMockResponse(fileName)));

    PromQLQuery query =
        PromQLQuery.builder()
            .query("errorCount")
            .query("numCall")
            .startTime(Instant.ofEpochMilli(1435781430000L))
            .endTime(Instant.ofEpochMilli(1435781460000L))
            .isInstantRequest(false)
            .step(Duration.of(15000, ChronoUnit.MILLIS))
            .build();

    Map<Request, PromQLMetricResponse> metricQueryResponses = prometheusRestClient.execute(query);

    Assertions.assertEquals(2, metricQueryResponses.size());
  }

  @Test
  public void testOneOfInstantQueryFail() throws IOException {
    List<String> files = List.of("promql_error_count_vector.json");
    files.stream().forEach(fileName -> mockWebServer.enqueue(getSuccessMockResponse(fileName)));
    mockWebServer.enqueue(getFailMockResponse("promql_num_call_vector.json"));

    PromQLQuery query =
        PromQLQuery.builder()
            .query("errorCount")
            .query("numCall")
            .endTime(Instant.ofEpochMilli(1435781451000L))
            .isInstantRequest(true)
            .build();

    Assertions.assertThrows(RuntimeException.class, () -> prometheusRestClient.execute(query));
  }

  private MockResponse getSuccessMockResponse(String fileName) {
    URL fileUrl = PrometheusRestClientTest.class.getClassLoader().getResource(fileName);
    String content = null;
    try {
      content = new String(Files.readAllBytes(Paths.get(fileUrl.getFile())));
    } catch (IOException ioException) {
      throw new RuntimeException(ioException);
    }
    return new MockResponse()
        .setResponseCode(200)
        .addHeader("Content-Type", "application/json")
        .setBody(content);
  }

  private MockResponse getFailMockResponse(String fileName) {
    return new MockResponse().setResponseCode(500);
  }
}
