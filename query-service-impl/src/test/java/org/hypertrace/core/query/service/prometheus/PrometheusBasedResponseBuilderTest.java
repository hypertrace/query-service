package org.hypertrace.core.query.service.prometheus;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import okhttp3.Request;
import org.hypertrace.core.query.service.api.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PrometheusBasedResponseBuilderTest {

  @Test
  public void testAggregationQueryResponse() throws IOException {
    Request request = getPromQLRequest("promql_error_count_vector.json");
    PromQLMetricResponse response = getPromQLMetricResponse("promql_error_count_vector.json");

    // map of query to prometheus response
    Map<Request, PromQLMetricResponse> promQLMetricResponseMap = Map.of(request, response);

    // map of attribute to metric attribute
    Map<String, String> metricAttributeMap =
        Map.of(
            "SERVICE.job", "job",
            "SERVICE.instance", "instance");

    // map of metric attribute to query
    Map<String, String> metricMap =
        Map.of("SERVICE.errorCount", getPromQLQuery("promql_error_count_vector.json"));

    LinkedHashSet<String> columnSet =
        new LinkedHashSet<>(List.of("SERVICE.job", "SERVICE.instance", "SERVICE.errorCount"));

    List<Row> rowList =
        PrometheusBasedResponseBuilder.buildResponse(
            promQLMetricResponseMap, metricAttributeMap, metricMap, columnSet, "SERVICE.startTime");

    Assertions.assertEquals(2, rowList.size());
  }

  @Test
  public void testTimeSeriesQueryResponse() throws IOException {
    Request request = getPromQLRequest("promql_error_count_matrix.json");
    PromQLMetricResponse response = getPromQLMetricResponse("promql_error_count_matrix.json");

    Map<Request, PromQLMetricResponse> promQLMetricResponseMap = Map.of(request, response);

    Map<String, String> metricAttributeMap =
        Map.of(
            "SERVICE.job", "job",
            "SERVICE.instance", "instance");

    Map<String, String> metricMap =
        Map.of("SERVICE.errorCount", getPromQLQuery("promql_error_count_matrix.json"));

    LinkedHashSet<String> columnSet =
        new LinkedHashSet<>(
            List.of("SERVICE.startTime", "SERVICE.job", "SERVICE.instance", "SERVICE.errorCount"));

    List<Row> rowList =
        PrometheusBasedResponseBuilder.buildResponse(
            promQLMetricResponseMap, metricAttributeMap, metricMap, columnSet, "SERVICE.startTime");

    Assertions.assertEquals(6, rowList.size());
  }

  @Test
  public void testMultipleAggregationQueryResponse() throws IOException {
    // First PromQL query and its response
    Request request1 = getPromQLRequest("promql_error_count_vector.json");
    PromQLMetricResponse response1 = getPromQLMetricResponse("promql_error_count_vector.json");

    // Second PromQL query and its response
    Request request2 = getPromQLRequest("promql_num_call_vector.json");
    PromQLMetricResponse response2 = getPromQLMetricResponse("promql_num_call_vector.json");

    Map<Request, PromQLMetricResponse> promQLMetricResponseMap =
        Map.of(
            request1, response1,
            request2, response2);

    Map<String, String> metricAttributeMap =
        Map.of(
            "SERVICE.job", "job",
            "SERVICE.instance", "instance");

    Map<String, String> metricMap =
        Map.of(
            "SERVICE.errorCount", getPromQLQuery("promql_error_count_vector.json"),
            "SERVICE.numCalls", getPromQLQuery("promql_num_call_vector.json"));

    LinkedHashSet<String> columnSet =
        new LinkedHashSet<>(
            List.of("SERVICE.job", "SERVICE.instance", "SERVICE.errorCount", "SERVICE.numCalls"));

    List<Row> rowList =
        PrometheusBasedResponseBuilder.buildResponse(
            promQLMetricResponseMap, metricAttributeMap, metricMap, columnSet, "SERVICE.startTime");

    Assertions.assertEquals(2, rowList.size());
  }

  @Test
  public void testMultipleTimeSeriesQueryResponse() throws IOException {
    // First PromQL query and its response
    Request request1 = getPromQLRequest("promql_error_count_matrix.json");
    PromQLMetricResponse response1 = getPromQLMetricResponse("promql_error_count_matrix.json");

    // Second PromQL query and its response
    Request request2 = getPromQLRequest("promql_num_call_matrix.json");
    PromQLMetricResponse response2 = getPromQLMetricResponse("promql_num_call_matrix.json");

    Map<Request, PromQLMetricResponse> promQLMetricResponseMap =
        Map.of(
            request1, response1,
            request2, response2);

    Map<String, String> metricAttributeMap =
        Map.of(
            "SERVICE.job", "job",
            "SERVICE.instance", "instance");

    Map<String, String> metricMap =
        Map.of(
            "SERVICE.errorCount", getPromQLQuery("promql_error_count_matrix.json"),
            "SERVICE.numCalls", getPromQLQuery("promql_num_call_matrix.json"));

    LinkedHashSet<String> columnSet =
        new LinkedHashSet<>(
            List.of(
                "SERVICE.startTime",
                "SERVICE.job",
                "SERVICE.instance",
                "SERVICE.errorCount",
                "SERVICE.numCalls"));

    List<Row> rowList =
        PrometheusBasedResponseBuilder.buildResponse(
            promQLMetricResponseMap, metricAttributeMap, metricMap, columnSet, "SERVICE.startTime");

    Assertions.assertEquals(6, rowList.size());
  }

  private PromQLMetricResponse getPromQLMetricResponse(String fileName) throws IOException {
    URL fileUrl = PrometheusBasedResponseBuilderTest.class.getClassLoader().getResource(fileName);
    String content = new String(Files.readAllBytes(Paths.get(fileUrl.getFile())));
    return PromQLMetricResponse.fromJson(content);
  }

  private Request getPromQLRequest(String fileName) throws IOException {
    return new Request.Builder()
        .url("http://localhost:9090/api/v1/query?query=" + getPromQLQuery(fileName))
        .build();
  }

  private String getPromQLQuery(String fileName) throws IOException {
    switch (fileName) {
      case "promql_error_count_vector.json":
        return "sum by (job, instance) (sum_over_time(errorCount{\"tenant_id\"=\"__default\"}[100ms]))";
      case "promql_error_count_matrix.json":
        return "sum by (job, instance) (sum_over_time(errorCount{\"tenant_id\"=\"__default\"}[15ms]))";
      case "promql_num_call_vector.json":
        return "sum by (job, instance) (sum_over_time(numCalls{\"tenant_id\"=\"__default\"}[100ms]))";
      case "promql_num_call_matrix.json":
        return "sum by (job, instance) (sum_over_time(numCalls{\"tenant_id\"=\"__default\"}[15ms]))";
      default:
        throw new IOException("matching request for resource not found");
    }
  }
}
