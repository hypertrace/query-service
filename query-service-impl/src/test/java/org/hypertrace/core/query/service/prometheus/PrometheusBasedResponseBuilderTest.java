package org.hypertrace.core.query.service.prometheus;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import okhttp3.Request;
import org.hypertrace.core.query.service.api.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PrometheusBasedResponseBuilderTest {

  /*
   * PromQL response
   * Map<columnName, metrics_attribute> (SERVICE.id, service_id)
   * Map<columnName, query_name> (SERVICE.numCalls, query) -> merticMap
   * columnSet : list of all selected columns request metatdata
   * <SERVICE.startTime, SERVICE.id, SERVICE.numCall, Service.errorCount> // columnSet
   * timeStampColumn : SERVICE.startTime
   * */

  @Test
  public void testAggregationQueryResponse() throws IOException {
    Request request =
        new Request.Builder()
            .url(
                "http://localhost:9090/api/v1/query?query=sum by (job, instance) (sum_over_time(errorCount{}[100ms]))")
            .build();

    URL fileUrl =
        PromQLMetricResponseTest.class
            .getClassLoader()
            .getResource("promql_error_count_vector.json");
    String content = new String(Files.readAllBytes(Paths.get(fileUrl.getFile())));
    PromQLMetricResponse response = PromQLMetricResponse.fromJson(content);

    Map<Request, PromQLMetricResponse> promQLMetricResponseMap = Map.of(request, response);
    Map<String, String> metricAttributeMap =
        Map.of(
            "SERVICE.job", "job",
            "SERVICE.instance", "instance");

    Map<String, String> metricMap =
        Map.of("SERVICE.errorCount", "sum by (job, instance) (sum_over_time(errorCount{}[100ms]))");

    List<String> columnSet = List.of("SERVICE.job", "SERVICE.instance", "SERVICE.errorCount");

    List<Row> rowList =
        PrometheusBasedResponseBuilder.buildResponse(
            promQLMetricResponseMap, metricAttributeMap, metricMap, columnSet, "SERVICE.startTime");

    Assertions.assertEquals(2, rowList.size());
  }


  @Test
  public void testTimeSeriesQueryResponse() throws IOException {
    Request request =
        new Request.Builder()
            .url(
                "http://localhost:9090/api/v1/query?query=sum by (job, instance) (sum_over_time(errorCount{}[15ms]))")
            .build();

    URL fileUrl =
        PromQLMetricResponseTest.class
            .getClassLoader()
            .getResource("promql_error_count_matrix.json");
    String content = new String(Files.readAllBytes(Paths.get(fileUrl.getFile())));
    PromQLMetricResponse response = PromQLMetricResponse.fromJson(content);

    Map<Request, PromQLMetricResponse> promQLMetricResponseMap = Map.of(request, response);
    Map<String, String> metricAttributeMap =
        Map.of(
            "SERVICE.job", "job",
            "SERVICE.instance", "instance");

    Map<String, String> metricMap =
        Map.of("SERVICE.errorCount", "sum by (job, instance) (sum_over_time(errorCount{}[15ms]))");

    List<String> columnSet = List.of("SERVICE.startTime", "SERVICE.job", "SERVICE.instance", "SERVICE.errorCount");

    List<Row> rowList =
        PrometheusBasedResponseBuilder.buildResponse(
            promQLMetricResponseMap, metricAttributeMap, metricMap, columnSet, "SERVICE.startTime");

    Assertions.assertEquals(6, rowList.size());
  }
}
