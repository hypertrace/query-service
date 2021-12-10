package org.hypertrace.core.query.service.prometheus;

import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createColumnExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createFunctionExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createTimeFilter;

import com.typesafe.config.Config;
import io.reactivex.rxjava3.core.Observable;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.QueryRequest.Builder;
import org.hypertrace.core.query.service.api.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class PrometheusBasedRequestHandlerTest {

  private static MockWebServer mockWebServer;
  private static PrometheusBasedRequestHandler prometheusBasedRequestHandler;

  private static final String CONFIG_PATH_NAME = "name";
  private static final String CONFIG_PATH_TYPE = "type";
  private static final String CONFIG_PATH_CLIENT_KEY = "clientConfig";
  private static final String CONFIG_PATH_REQUEST_HANDLER_INFO = "requestHandlerInfo";

  @BeforeAll
  public static void setUp() throws IOException {
    mockWebServer = new MockWebServer();
    mockWebServer.start(9099);

    Config config = PrometheusTestUtils.getDefaultPrometheusConfig();
    prometheusBasedRequestHandler =
        new PrometheusBasedRequestHandler(
            config.getString(CONFIG_PATH_NAME),
            config.getConfig(CONFIG_PATH_REQUEST_HANDLER_INFO),
            config.getString(CONFIG_PATH_CLIENT_KEY));
  }

  @AfterAll
  public static void tearDown() throws IOException {
    mockWebServer.shutdown();
  }

  @Test
  public void test() throws IOException {
    QueryRequest request = buildSingleAggQuery();

    MockResponse mockResponse = getSuccessMockResponse("promql_error_count_service_vector.json");
    mockWebServer.enqueue(mockResponse);

    ExecutionContext executionContext = new ExecutionContext("__default", request);
    executionContext.setTimeFilterColumn("SERVICE.startTime");

    Observable<Row> rowObservable =
        prometheusBasedRequestHandler.handleRequest(request, executionContext);

    List<Row> rowList = rowObservable.toList().blockingGet();

    Assertions.assertEquals(
        2, executionContext.getResultSetMetadata().getColumnMetadataList().size());

    Assertions.assertEquals(2, rowList.size());
    Assertions.assertEquals(
        executionContext.getResultSetMetadata().getColumnMetadataList().size(),
        rowList.get(0).getColumnList().size());
  }

  /*
   * select SERVICE.name, SUM(SERVICE.errorCount) from timeRange GroupBy SERVICE.name
   * */
  private QueryRequest buildSingleAggQuery() {
    Builder builder = QueryRequest.newBuilder();
    builder.addAggregation(
        createFunctionExpression("SUM", createColumnExpression("SERVICE.errorCount").build()));

    Filter startTimeFilter = createTimeFilter("SERVICE.startTime", Operator.GT, 100L);
    Filter endTimeFilter = createTimeFilter("SERVICE.startTime", Operator.LT, 200L);

    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .build();
    builder.setFilter(andFilter);
    builder.addGroupBy(createColumnExpression("SERVICE.name"));
    return builder.build();
  }

  private MockResponse getSuccessMockResponse(String fileName) throws IOException {
    URL fileUrl = PrometheusRestClientTest.class.getClassLoader().getResource(fileName);
    String content = new String(Files.readAllBytes(Paths.get(fileUrl.getFile())));
    return new MockResponse()
        .setResponseCode(200)
        .addHeader("Content-Type", "application/json")
        .setBody(content);
  }
}
