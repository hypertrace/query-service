package org.hypertrace.core.query.service.pinot;

import static java.util.Objects.requireNonNull;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createAliasedFunctionExpressionWithSimpleAttribute;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createCountByColumnSelectionWithSimpleAttribute;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createFunctionExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createOrderByExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createSimpleAttributeExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createStringArrayLiteralValueExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createTimeFilterWithSimpleAttribute;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.Request;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.QueryRequest.Builder;
import org.hypertrace.core.query.service.api.SortOrder;
import org.hypertrace.core.query.service.pinot.PinotClientFactory.PinotClient;
import org.hypertrace.core.query.service.pinot.converters.PinotFunctionConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class MigrationTest {

  private static final String TENANT_ID = "__default";
  private static final String TENANT_COLUMN_NAME = "tenant_id";

  private static final String TEST_REQUEST_HANDLER_CONFIG_FILE = "request_handler.conf";
  private static final String TEST_SERVICE_REQUEST_HANDLER_CONFIG_FILE =
      "service_request_handler.conf";

  private Connection connection;
  private ExecutionContext executionContext;

  @BeforeEach
  public void setup() {
    executionContext = mock(ExecutionContext.class);
    connection = mock(Connection.class);
    Mockito.when(connection.prepareStatement(any(Request.class))).thenCallRealMethod();
  }

  @Test
  public void testQuery() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createSimpleAttributeExpression("Span.id").build());
    builder.addSelection(createSimpleAttributeExpression("Span.tags").build());
    builder.addSelection(
        createSimpleAttributeExpression("Span.attributes.request_headers").build());

    Filter startTimeFilter =
        createTimeFilterWithSimpleAttribute("Span.start_time_millis", Operator.GT, 1557780911508L);
    Filter endTimeFilter =
        createTimeFilterWithSimpleAttribute("Span.end_time_millis", Operator.LT, 1557780938419L);

    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .build();
    builder.setFilter(andFilter);

    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        builder.build(),
        "select span_id, tags__keys, tags__values, request_headers__keys, request_headers__values "
            + "from SpanEventView "
            + "where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "and ( start_time_millis > 1557780911508 and end_time_millis < 1557780938419 )",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryMultipleDistinctSelection() {
    Builder builder = QueryRequest.newBuilder();
    builder
        .setDistinctSelections(true)
        .addSelection(createSimpleAttributeExpression("Span.id"))
        .addSelection(createSimpleAttributeExpression("Span.displaySpanName"))
        .addSelection(createSimpleAttributeExpression("Span.serviceName"));
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        builder.build(),
        "Select distinct span_id, span_name, service_name FROM SpanEventView "
            + "where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "'",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithOrderBy() {
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        buildOrderByQuery(),
        "Select span_id, start_time_millis, end_time_millis FROM SpanEventView WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "order by start_time_millis desc , end_time_millis limit 100",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithOrderByWithPagination() {
    QueryRequest orderByQueryRequest = buildOrderByQuery();
    Builder builder = QueryRequest.newBuilder(orderByQueryRequest);
    builder.setOffset(1000);
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        builder.build(),
        "Select span_id, start_time_millis, end_time_millis FROM SpanEventView WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "order by start_time_millis desc , end_time_millis limit 1000, 100",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithGroupByWithMultipleAggregates() {
    QueryRequest orderByQueryRequest = buildMultipleGroupByMultipleAggQuery();
    Builder builder = QueryRequest.newBuilder(orderByQueryRequest);
    builder.setLimit(20);
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        builder.build(),
        "select service_name, span_name, count(*), avg(duration_millis) from SpanEventView"
            + " where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "and ( start_time_millis > 1570658506605 and end_time_millis < 1570744906673 )"
            + " group by service_name, span_name limit 20",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithGroupByWithMultipleAggregatesAndOrderBy() {
    QueryRequest orderByQueryRequest = buildMultipleGroupByMultipleAggAndOrderByQuery();
    Builder builder = QueryRequest.newBuilder(orderByQueryRequest);
    builder.setLimit(20);
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        builder.build(),
        "select service_name, span_name, count(*), avg(duration_millis) from SpanEventView"
            + " where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "and ( start_time_millis > 1570658506605 and end_time_millis < 1570744906673 )"
            + " group by service_name, span_name order by service_name, avg(duration_millis) desc , count(*) desc  limit 20",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithDistinctCountAggregation() {
    Filter startTimeFilter =
        createTimeFilterWithSimpleAttribute("Span.start_time_millis", Operator.GT, 1570658506605L);
    Filter endTimeFilter =
        createTimeFilterWithSimpleAttribute("Span.end_time_millis", Operator.LT, 1570744906673L);
    QueryRequest queryRequest =
        QueryRequest.newBuilder()
            .addAggregation(
                createAliasedFunctionExpressionWithSimpleAttribute(
                    "DISTINCTCOUNT", "Span.id", "distinctcount_span_id"))
            .setFilter(
                Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addChildFilter(startTimeFilter)
                    .addChildFilter(endTimeFilter)
                    .build())
            .setLimit(15)
            .build();

    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        queryRequest,
        "select distinctcount(span_id) from SpanEventView"
            + " where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "and ( start_time_millis > 1570658506605 and end_time_millis < 1570744906673 )"
            + " limit 15",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithContainsKeyValueOperator() {
    Builder builder = QueryRequest.newBuilder();
    Expression spanTag = createSimpleAttributeExpression("Span.tags").build();
    builder.addSelection(spanTag);

    Expression tag = createStringArrayLiteralValueExpression(List.of("FLAGS", "0"));
    Filter likeFilter =
        Filter.newBuilder()
            .setOperator(Operator.CONTAINS_KEYVALUE)
            .setLhs(spanTag)
            .setRhs(tag)
            .build();
    builder.setFilter(likeFilter);

    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        builder.build(),
        "SELECT tags__keys, tags__values FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND tags__keys = 'flags' and tags__values = '0' and mapvalue(tags__keys,'flags',tags__values) = '0'",
        viewDefinition,
        executionContext);
  }

  private QueryRequest buildOrderByQuery() {
    Builder builder = QueryRequest.newBuilder();
    Expression startTimeColumn = createSimpleAttributeExpression("Span.start_time_millis").build();
    Expression endTimeColumn = createSimpleAttributeExpression("Span.end_time_millis").build();

    builder.addSelection(createSimpleAttributeExpression("Span.id"));
    builder.addSelection(startTimeColumn);
    builder.addSelection(endTimeColumn);

    builder.addOrderBy(createOrderByExpression(startTimeColumn.toBuilder(), SortOrder.DESC));
    builder.addOrderBy(createOrderByExpression(endTimeColumn.toBuilder(), SortOrder.ASC));

    builder.setLimit(100);
    return builder.build();
  }

  private QueryRequest buildMultipleGroupByMultipleAggQuery() {
    Builder builder = QueryRequest.newBuilder();
    builder.addAggregation(createCountByColumnSelectionWithSimpleAttribute("Span.id"));
    Expression avg =
        createFunctionExpression(
            "AVG", createSimpleAttributeExpression("Span.duration_millis").build());
    builder.addAggregation(avg);

    Filter startTimeFilter =
        createTimeFilterWithSimpleAttribute("Span.start_time_millis", Operator.GT, 1570658506605L);
    Filter endTimeFilter =
        createTimeFilterWithSimpleAttribute("Span.end_time_millis", Operator.LT, 1570744906673L);

    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .build();
    builder.setFilter(andFilter);

    builder.addGroupBy(createSimpleAttributeExpression("Span.serviceName"));
    builder.addGroupBy(createSimpleAttributeExpression("Span.displaySpanName"));
    return builder.build();
  }

  private QueryRequest buildMultipleGroupByMultipleAggAndOrderByQuery() {
    Builder builder = QueryRequest.newBuilder();
    builder.addAggregation(createCountByColumnSelectionWithSimpleAttribute("Span.id"));
    Expression avg =
        createFunctionExpression(
            "AVG", createSimpleAttributeExpression("Span.duration_millis").build());
    builder.addAggregation(avg);

    Filter startTimeFilter =
        createTimeFilterWithSimpleAttribute("Span.start_time_millis", Operator.GT, 1570658506605L);
    Filter endTimeFilter =
        createTimeFilterWithSimpleAttribute("Span.end_time_millis", Operator.LT, 1570744906673L);

    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .build();
    builder.setFilter(andFilter);

    builder.addGroupBy(createSimpleAttributeExpression("Span.serviceName"));
    builder.addGroupBy(createSimpleAttributeExpression("Span.displaySpanName"));

    builder.addOrderBy(
        createOrderByExpression(
            createSimpleAttributeExpression("Span.serviceName"), SortOrder.ASC));
    builder.addOrderBy(
        createOrderByExpression(
            createAliasedFunctionExpressionWithSimpleAttribute(
                "AVG", "Span.duration_millis", "avg_duration_millis"),
            SortOrder.DESC));
    builder.addOrderBy(
        createOrderByExpression(
            createAliasedFunctionExpressionWithSimpleAttribute("COUNT", "Span.id", "count_span_id"),
            SortOrder.DESC));
    return builder.build();
  }

  private void assertPQLQuery(
      QueryRequest queryRequest,
      String expectedQuery,
      ViewDefinition viewDefinition,
      ExecutionContext executionContext) {
    QueryRequestToPinotSQLConverter converter =
        new QueryRequestToPinotSQLConverter(viewDefinition, new PinotFunctionConverter());
    Entry<String, Params> statementToParam =
        converter.toSQL(
            executionContext, queryRequest, createSelectionsFromQueryRequest(queryRequest));
    PinotClient pinotClient = new PinotClient(connection);
    pinotClient.executeQuery(statementToParam.getKey(), statementToParam.getValue());
    ArgumentCaptor<Request> statementCaptor = ArgumentCaptor.forClass(Request.class);
    Mockito.verify(connection, Mockito.times(1)).execute(statementCaptor.capture());
    Assertions.assertEquals(
        expectedQuery.toLowerCase(), statementCaptor.getValue().getQuery().toLowerCase());
  }

  // This method will put the selections in a LinkedHashSet in the order that RequestAnalyzer does:
  // group bys,
  // selections then aggregations.
  private LinkedHashSet<Expression> createSelectionsFromQueryRequest(QueryRequest queryRequest) {
    LinkedHashSet<Expression> selections = new LinkedHashSet<>();

    selections.addAll(queryRequest.getGroupByList());
    selections.addAll(queryRequest.getSelectionList());
    selections.addAll(queryRequest.getAggregationList());

    return selections;
  }

  private ViewDefinition getDefaultViewDefinition() {
    Config fileConfig =
        ConfigFactory.parseURL(
            requireNonNull(
                QueryRequestToPinotSQLConverterTest.class
                    .getClassLoader()
                    .getResource(TEST_REQUEST_HANDLER_CONFIG_FILE)));

    return ViewDefinition.parse(
        fileConfig.getConfig("requestHandlerInfo.viewDefinition"), TENANT_COLUMN_NAME);
  }

  private void defaultMockingForExecutionContext() {
    when(executionContext.getTenantId()).thenReturn("__default");
  }
}
