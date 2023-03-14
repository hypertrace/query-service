package org.hypertrace.core.query.service.postgres;

import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createTimeFilter;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.QueryRequestBuilderUtils;
import org.hypertrace.core.query.service.RequestHandler;
import org.hypertrace.core.query.service.RequestHandlerRegistry;
import org.hypertrace.core.query.service.RequestHandlerSelector;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PostgresBasedRequestHandlerTest {
  private final Config serviceConfig =
      ConfigFactory.parseURL(
              Objects.requireNonNull(
                  QueryRequestToPostgresSQLConverterTest.class
                      .getClassLoader()
                      .getResource("application.conf")))
          .getConfig("service.config");
  private final Set<RequestHandler> requestHandlers = preparePostgresBasedRequestHandler();

  @Test
  /**
   * Test the scenarios among handlers where range fits between two minRequestTime:[ actual <
   * minReqTime1 < minReqTime2 < minReqTime3 ]
   */
  public void testOneHourQueryRequestTimeRangeRequestHandler() {
    RequestHandlerRegistry mockRegistry = mock(RequestHandlerRegistry.class);
    RequestHandlerSelector requestHandlerSelector = new RequestHandlerSelector(mockRegistry);
    when(mockRegistry.getAll()).thenReturn(requestHandlers);

    // prepare 1 hrs query range
    QueryRequest request = prepareOneHourTimeRangeQueryRequest();
    ExecutionContext context = new ExecutionContext("__default", request);

    // select an handler
    Optional<RequestHandler> selectedRequestHandler =
        requestHandlerSelector.select(request, context);

    // verify that default handler is selected
    Assertions.assertFalse(selectedRequestHandler.isEmpty());
    Assertions.assertEquals(
        "backend-traces-from-bare-span-event-view-aggr-handler",
        selectedRequestHandler.get().getName());
  }

  @Test
  /**
   * Test the scenarios among handlers where range fits between two minRequestTime:[ minReqTime1 <
   * minReqTime2 < minReqTime3 < actual ]
   */
  public void testTwelveHoursQueryRequestTimeRangeRequestHandler() {
    RequestHandlerRegistry mockRegistry = mock(RequestHandlerRegistry.class);
    RequestHandlerSelector requestHandlerSelector = new RequestHandlerSelector(mockRegistry);
    when(mockRegistry.getAll()).thenReturn(requestHandlers);

    // prepare 12 hrs query range
    QueryRequest request = prepareTwelveHourTimeRangeQueryRequest();
    ExecutionContext context = new ExecutionContext("__default", request);

    // select an handler
    Optional<RequestHandler> selectedRequestHandler =
        requestHandlerSelector.select(request, context);

    // verify that default handler is selected
    Assertions.assertFalse(selectedRequestHandler.isEmpty());
    Assertions.assertEquals(
        "backend-traces-from-bare-span-event-view-5min-aggr-handler",
        selectedRequestHandler.get().getName());
  }

  @Test
  /**
   * Test the scenarios among handlers where range fits between two minRequestTime:[ minReqTime1 <
   * minReqTime2 < actual < minReqTime2 ]
   */
  public void testFourHoursQueryRequestTimeRangeRequestHandler() {
    RequestHandlerRegistry mockRegistry = mock(RequestHandlerRegistry.class);
    RequestHandlerSelector requestHandlerSelector = new RequestHandlerSelector(mockRegistry);
    when(mockRegistry.getAll()).thenReturn(requestHandlers);

    // prepare 4 hrs query range
    QueryRequest request = prepareFourHourTimeRangeQueryRequest();
    ExecutionContext context = new ExecutionContext("__default", request);

    // select an handler
    Optional<RequestHandler> selectedRequestHandler =
        requestHandlerSelector.select(request, context);

    // verify that default handler is selected
    Assertions.assertFalse(selectedRequestHandler.isEmpty());
    Assertions.assertEquals(
        "backend-traces-from-bare-span-event-view-3hrs-aggr-handler",
        selectedRequestHandler.get().getName());
  }

  private boolean isPostgresConfig(Config config) {
    return config.getString("type").equals("postgres");
  }

  private Set<RequestHandler> preparePostgresBasedRequestHandler() {
    Set<RequestHandler> requestHandlers = new LinkedHashSet<>();
    for (Config config : serviceConfig.getConfigList("queryRequestHandlersConfig")) {
      if (!isPostgresConfig(config)) {
        continue;
      }
      PostgresBasedRequestHandler handler =
          new PostgresBasedRequestHandler(
              config.getString("name"), config.getConfig("requestHandlerInfo"));
      requestHandlers.add(handler);
    }
    return requestHandlers;
  }

  private QueryRequest prepareOneHourTimeRangeQueryRequest() {
    QueryRequest.Builder builder = QueryRequest.newBuilder();
    long startTimeInMillis = System.currentTimeMillis();
    Filter startTimeFilter =
        createTimeFilter("BACKEND_TRACE.startTime", Operator.GT, startTimeInMillis);
    Filter endTimeFilter =
        createTimeFilter(
            "BACKEND_TRACE.startTime",
            Operator.LT,
            startTimeInMillis + Duration.ofHours(1).toMillis());

    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .build();
    builder.setFilter(andFilter);
    builder.addSelection(
        QueryRequestBuilderUtils.createColumnExpression("BACKEND_TRACE.backendId"));

    return builder.build();
  }

  private QueryRequest prepareFourHourTimeRangeQueryRequest() {
    QueryRequest.Builder builder = QueryRequest.newBuilder();
    long startTimeInMillis = System.currentTimeMillis();
    Filter startTimeFilter =
        createTimeFilter("BACKEND_TRACE.startTime", Operator.GT, startTimeInMillis);
    Filter endTimeFilter =
        createTimeFilter(
            "BACKEND_TRACE.startTime",
            Operator.LT,
            startTimeInMillis + Duration.ofHours(4).toMillis());

    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .build();
    builder.setFilter(andFilter);
    builder.addSelection(
        QueryRequestBuilderUtils.createColumnExpression("BACKEND_TRACE.backendId"));

    return builder.build();
  }

  private QueryRequest prepareTwelveHourTimeRangeQueryRequest() {
    QueryRequest.Builder builder = QueryRequest.newBuilder();
    long startTimeInMillis = System.currentTimeMillis();
    Filter startTimeFilter =
        createTimeFilter("BACKEND_TRACE.startTime", Operator.GT, startTimeInMillis);
    Filter endTimeFilter =
        createTimeFilter(
            "BACKEND_TRACE.startTime",
            Operator.LT,
            startTimeInMillis + Duration.ofHours(12).toMillis());

    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .build();
    builder.setFilter(andFilter);
    builder.addSelection(
        QueryRequestBuilderUtils.createColumnExpression("BACKEND_TRACE.backendId"));

    return builder.build();
  }
}
