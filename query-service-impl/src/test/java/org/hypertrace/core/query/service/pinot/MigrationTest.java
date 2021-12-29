package org.hypertrace.core.query.service.pinot;

import static java.util.Objects.requireNonNull;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createAliasedFunctionExpressionWithSimpleAttribute;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createColumnExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createComplexAttributeExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createCompositeFilter;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createCountByColumnSelectionWithSimpleAttribute;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createFunctionExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createNullStringLiteralValueExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createOrderByExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createStringArrayLiteralValueExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createTimeFilterWithSimpleAttribute;
import static org.hypertrace.core.query.service.QueryRequestUtil.createContainsKeyFilter;
import static org.hypertrace.core.query.service.QueryRequestUtil.createSimpleAttributeExpression;
import static org.hypertrace.core.query.service.QueryRequestUtil.createStringLiteralValueExpression;
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
  public void testQuerySelectionUsingAggregateFunctionUsingMapAttribute() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(
        createFunctionExpression(
            "sum", createComplexAttributeExpression("Span.tags", "otel.status_code").build()));
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        builder.build(),
        "Select sum(mapValue(tags__KEYS,'otel.status_code',tags__VALUES)) FROM SpanEventView "
            + "where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "'",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQuerySelectionUsingMapAttributeWithSubPath() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createComplexAttributeExpression("Span.tags", "span.kind"));
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        builder.build(),
        "Select mapValue(tags__KEYS,'span.kind',tags__VALUES) FROM SpanEventView "
            + "where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "'",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQuerySelectionUsingMapAttributeWithoutSubPath() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createSimpleAttributeExpression("Span.tags"));
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        builder.build(),
        "Select tags__KEYS, tags__VALUES FROM SpanEventView "
            + "where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "'",
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
  public void testQueryWithLikeOperator() {
    Builder builder = QueryRequest.newBuilder();
    Expression spanId = createSimpleAttributeExpression("Span.id").build();
    builder.addSelection(spanId);

    Filter likeFilter =
        Filter.newBuilder()
            .setOperator(Operator.LIKE)
            .setLhs(spanId)
            .setRhs(createStringLiteralValueExpression("042e5523ff6b2506"))
            .build();
    builder.setFilter(likeFilter);

    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        builder.build(),
        "SELECT span_id FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND REGEXP_LIKE(span_id,'042e5523ff6b2506')",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithNotContainsKeyOperator() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.tags"));
    builder.setFilter(
        Filter.newBuilder()
            .setOperator(Operator.NOT_CONTAINS_KEY)
            .setLhs(createSimpleAttributeExpression("Span.tags"))
            .setRhs(createStringLiteralValueExpression("Flags"))
            .build());

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
            + "AND tags__keys != 'flags'",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithContainsKeyValueOperator() {
    Builder builder = QueryRequest.newBuilder();
    Expression spanTag = createSimpleAttributeExpression("Span.tags").build();
    builder.addSelection(spanTag);
    builder.setFilter(
        Filter.newBuilder()
            .setOperator(Operator.CONTAINS_KEYVALUE)
            .setLhs(spanTag)
            .setRhs(createStringArrayLiteralValueExpression(List.of("Flags", "0")))
            .build());

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
            + "AND tags__keys = 'flags' and tags__values = '0' "
            + "AND mapvalue(tags__keys,'flags',tags__values) = '0'",
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
  public void testQueryWithEQFilterForMapAttribute() {
    Builder builder = QueryRequest.newBuilder();
    Expression spanTag = createComplexAttributeExpression("Span.tags", "FLAGS").build();
    builder.addSelection(spanTag);

    Filter equalFilter =
        Filter.newBuilder()
            .setOperator(Operator.EQ)
            .setLhs(spanTag)
            .setRhs(createStringLiteralValueExpression("0"))
            .build();
    builder.setFilter(
        createCompositeFilter(
            Operator.AND, createContainsKeyFilter("Span.tags", "FLAGS"), equalFilter));

    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        builder.build(),
        "SELECT mapValue(tags__keys,'flags',tags__values) FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND ( tags__keys = 'flags' and mapvalue(tags__keys,'flags',tags__values) = '0' )",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithGTFilterForMapAttribute() {
    Builder builder = QueryRequest.newBuilder();
    Expression spanKind = createComplexAttributeExpression("Span.tags", "span.kind").build();
    builder.addSelection(spanKind);

    Filter greaterThanFilter =
        Filter.newBuilder()
            .setOperator(Operator.GT)
            .setLhs(spanKind)
            .setRhs(createStringLiteralValueExpression("client"))
            .build();
    builder.setFilter(
        createCompositeFilter(
            Operator.AND, createContainsKeyFilter("Span.tags", "span.kind"), greaterThanFilter));

    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        builder.build(),
        "SELECT mapValue(tags__keys,'span.kind',tags__values) FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND ( tags__keys = 'span.kind' and mapvalue(tags__keys,'span.kind',tags__values) > 'client' )",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithLikeOperatorForMapAttribute() {
    Builder builder = QueryRequest.newBuilder();
    Expression spanKind = createComplexAttributeExpression("Span.tags", "span.kind").build();
    builder.addSelection(spanKind);

    Filter notEqualsFilter =
        Filter.newBuilder()
            .setOperator(Operator.NEQ)
            .setLhs(spanKind)
            .setRhs(createNullStringLiteralValueExpression())
            .build();

    Filter likeFilter =
        Filter.newBuilder()
            .setOperator(Operator.LIKE)
            .setLhs(spanKind)
            .setRhs(createStringLiteralValueExpression("^client*"))
            .build();

    Filter containsKeyFilter = createContainsKeyFilter(spanKind.getAttributeExpression());

    builder.setFilter(
        createCompositeFilter(Operator.AND, containsKeyFilter, notEqualsFilter, likeFilter));

    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        builder.build(),
        "SELECT mapValue(tags__KEYS,'span.kind',tags__VALUES) FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND ( tags__KEYS = 'span.kind' "
            + "AND mapValue(tags__KEYS,'span.kind',tags__VALUES) != 'null' "
            + "AND REGEXP_LIKE(mapValue(tags__KEYS,'span.kind',tags__VALUES),'^client*') )",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithOrderByWithMapAttribute() {
    Builder builder = QueryRequest.newBuilder();
    Expression spanKind = createComplexAttributeExpression("Span.tags", "span.kind").build();
    builder.addSelection(spanKind);

    Filter greaterThanOrEqualToFilter =
        Filter.newBuilder()
            .setOperator(Operator.GE)
            .setLhs(spanKind)
            .setRhs(createStringLiteralValueExpression("client"))
            .build();
    builder.setFilter(
        createCompositeFilter(
            Operator.AND,
            createContainsKeyFilter("Span.tags", "span.kind"),
            greaterThanOrEqualToFilter));
    builder.addOrderBy(createOrderByExpression(spanKind.toBuilder(), SortOrder.DESC));

    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        builder.build(),
        "select mapValue(tags__KEYS,'span.kind',tags__VALUES) FROM spanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND ( tags__keys = 'span.kind' and mapvalue(tags__keys,'span.kind',tags__values) >= 'client' ) "
            + "order by mapvalue(tags__KEYS,'span.kind',tags__VALUES) "
            + "DESC ",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithGroupByWithMapAttribute() {
    Builder builder = QueryRequest.newBuilder(buildGroupByMapAttributeQuery());
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();
    assertPQLQuery(
        builder.build(),
        "select mapValue(tags__KEYS,'span.kind',tags__VALUES), AVG(duration_millis) FROM spanEventView"
            + " where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND ( start_time_millis > 1570658506605 AND start_time_millis < 1570744906673 "
            + "AND tags__keys = 'span.kind' "
            + "AND mapValue(tags__KEYS,'span.kind',tags__VALUES) != '' ) "
            + "group by mapValue(tags__KEYS,'span.kind',tags__VALUES)",
        viewDefinition,
        executionContext);
  }

  private QueryRequest buildGroupByMapAttributeQuery() {
    Builder builder = QueryRequest.newBuilder();

    Filter startTimeFilter =
        createTimeFilterWithSimpleAttribute("Span.start_time_millis", Operator.GT, 1570658506605L);
    Filter endTimeFilter =
        createTimeFilterWithSimpleAttribute("Span.start_time_millis", Operator.LT, 1570744906673L);
    Filter neqFilter =
        Filter.newBuilder()
            .setLhs(createComplexAttributeExpression("Span.tags", "span.kind"))
            .setOperator(Operator.NEQ)
            .setRhs(createStringLiteralValueExpression(""))
            .build();
    Filter containsKeyFilter = createContainsKeyFilter("Span.tags", "span.kind");

    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .addChildFilter(containsKeyFilter)
            .addChildFilter(neqFilter)
            .build();
    builder.setFilter(andFilter);

    Expression avg =
        createAliasedFunctionExpressionWithSimpleAttribute(
                "AVG", "Span.duration_millis", "avg_duration")
            .build();
    builder.addSelection(avg);

    Expression mapAttributeSelection =
        createComplexAttributeExpression("Span.tags", "span.kind").build();
    builder.addSelection(mapAttributeSelection);

    builder.addGroupBy(mapAttributeSelection);
    return builder.build();
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
