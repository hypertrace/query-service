package org.hypertrace.core.query.service.pinot;

import static java.util.Objects.requireNonNull;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createAliasedFunctionExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createColumnExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createContainsKeyFilter;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createCountByColumnSelection;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createEqualsFilter;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createFunctionExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createInFilter;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createLongLiteralValueExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createNotEqualsFilter;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createNullNumberLiteralValueExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createNullStringFilter;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createNullStringLiteralValueExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createOrderByExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createStringArrayLiteralValueExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createStringLiteralValueExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createTimeFilter;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createTimestampFilter;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.Request;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.QueryFunctionConstants;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Function;
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

public class QueryRequestToPinotSQLConverterTest {

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
    builder.addSelection(createColumnExpression("Span.id").build());
    builder.addSelection(createColumnExpression("Span.tags").build());
    builder.addSelection(createColumnExpression("Span.attributes.request_headers").build());

    Filter startTimeFilter =
        createTimeFilter("Span.start_time_millis", Operator.GT, 1557780911508L);
    Filter endTimeFilter = createTimeFilter("Span.end_time_millis", Operator.LT, 1557780938419L);

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
  public void testQueryWithoutFilter() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id").build());
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        builder.build(),
        "Select span_id FROM SpanEventView "
            + "where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "'",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQuerySingleDistinctSelection() {
    Builder builder = QueryRequest.newBuilder();
    builder.setDistinctSelections(true).addSelection(createColumnExpression("Span.id"));
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        builder.build(),
        "Select distinct span_id FROM SpanEventView "
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
        .addSelection(createColumnExpression("Span.id"))
        .addSelection(createColumnExpression("Span.displaySpanName"))
        .addSelection(createColumnExpression("Span.serviceName"));
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
  public void testQueryWithStringFilter() {
    QueryRequest queryRequest =
        buildSimpleQueryWithFilter(createEqualsFilter("Span.displaySpanName", "GET /login"));
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        queryRequest,
        "Select span_id FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND span_name = 'GET /login'",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testSQLiWithStringValueFilter() {
    QueryRequest queryRequest =
        buildSimpleQueryWithFilter(
            createEqualsFilter("Span.displaySpanName", "GET /login' OR tenant_id = 'tenant2"));
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        queryRequest,
        "Select span_id FROM SpanEventView WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND span_name = 'GET /login'' OR tenant_id = ''tenant2'",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithBooleanFilter() {
    QueryRequest queryRequest =
        buildSimpleQueryWithFilter(createEqualsFilter("Span.is_entry", true));
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        queryRequest,
        "Select span_id FROM SpanEventView WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND is_entry = 'true'",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithDoubleFilter() {
    QueryRequest queryRequest =
        buildSimpleQueryWithFilter(createEqualsFilter("Span.metrics.duration_millis", 1.2));
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        queryRequest,
        "Select span_id FROM SpanEventView WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND duration_millis = 1.2",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithFloatFilter() {
    QueryRequest queryRequest =
        buildSimpleQueryWithFilter(createEqualsFilter("Span.metrics.duration_millis", 1.2f));
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        queryRequest,
        "Select span_id FROM SpanEventView WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND duration_millis = 1.2",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithIntFilter() {
    QueryRequest queryRequest =
        buildSimpleQueryWithFilter(createEqualsFilter("Span.metrics.duration_millis", 1));
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        queryRequest,
        "Select span_id FROM SpanEventView WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND duration_millis = 1",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithTimestampFilter() {
    QueryRequest queryRequest =
        buildSimpleQueryWithFilter(createTimestampFilter("Span.is_entry", Operator.EQ, 123456));
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        queryRequest,
        "Select span_id FROM SpanEventView WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND is_entry = 123456",
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
        createTimeFilter("Span.start_time_millis", Operator.GT, 1570658506605L);
    Filter endTimeFilter = createTimeFilter("Span.end_time_millis", Operator.LT, 1570744906673L);
    QueryRequest queryRequest =
        QueryRequest.newBuilder()
            .addAggregation(
                createAliasedFunctionExpression(
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
  public void testQueryWithDistinctCountAggregationAndGroupBy() {
    Filter startTimeFilter =
        createTimeFilter("Span.start_time_millis", Operator.GT, 1570658506605L);
    Filter endTimeFilter = createTimeFilter("Span.end_time_millis", Operator.LT, 1570744906673L);
    QueryRequest queryRequest =
        QueryRequest.newBuilder()
            .addSelection(createColumnExpression("Span.id"))
            .addGroupBy(createColumnExpression("Span.id"))
            .addAggregation(
                createAliasedFunctionExpression(
                    "DISTINCTCOUNT", "Span.id", "distinctcount_span_id"))
            .setFilter(
                Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addChildFilter(startTimeFilter)
                    .addChildFilter(endTimeFilter)
                    .build())
            .addOrderBy(
                createOrderByExpression(
                    createAliasedFunctionExpression(
                        "DISTINCTCOUNT", "Span.id", "distinctcount_span_id"),
                    SortOrder.ASC))
            .setLimit(15)
            .build();

    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        queryRequest,
        "select span_id, distinctcount(span_id) from SpanEventView"
            + " where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "and ( start_time_millis > 1570658506605 and end_time_millis < 1570744906673 )"
            + " group by span_id order by distinctcount(span_id) limit 15",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithStringArray() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id").build());

    String spanId1 = "042e5523ff6b2506";
    String spanId2 = "041e5523ff6b2501";
    Filter filter = createInFilter("Span.id", List.of(spanId1, spanId2));
    builder.setFilter(filter);

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
            + "AND span_id IN ('"
            + spanId1
            + "', '"
            + spanId2
            + "')",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testSQLiWithStringArrayFilter() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.displaySpanName"));

    Filter filter =
        createInFilter(
            "Span.displaySpanName", List.of("1') OR tenant_id = 'tenant2' and span_name IN ('1"));
    builder.setFilter(filter);

    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        builder.build(),
        "SELECT span_name FROM SpanEventView WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND span_name IN ('1'') OR tenant_id = ''tenant2'' and span_name IN (''1')",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithLikeOperator() {
    Builder builder = QueryRequest.newBuilder();
    Expression spanId = createColumnExpression("Span.id").build();
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
  public void testQueryWithContainsKeyOperator() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.tags"));
    builder.setFilter(createContainsKeyFilter("Span.tags", List.of("FLAGS")));

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
            + "AND tags__keys = 'flags'",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithContainsKeyValueOperator() {
    Builder builder = QueryRequest.newBuilder();
    Expression spanTag = createColumnExpression("Span.tags").build();
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

  @Test
  public void testQueryWithBytesColumnWithValidId() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id").build());

    Filter parentIdFilter =
        createEqualsFilter("Span.attributes.parent_span_id", "042e5523ff6b2506");
    Filter andFilter =
        Filter.newBuilder().setOperator(Operator.AND).addChildFilter(parentIdFilter).build();
    builder.setFilter(andFilter);
    builder.setLimit(5);

    QueryRequest request = builder.build();
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        request,
        "SELECT span_id FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND ( parent_span_id = '042e5523ff6b2506' ) limit 5",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithBytesColumnWithInValidId() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id").build());

    Filter parentIdFilter =
        createEqualsFilter("Span.attributes.parent_span_id", "042e5523ff6b250L");
    Filter andFilter =
        Filter.newBuilder().setOperator(Operator.AND).addChildFilter(parentIdFilter).build();
    builder.setFilter(andFilter);
    builder.setLimit(5);

    assertExceptionOnPQLQuery(
        builder.build(),
        IllegalArgumentException.class,
        "Invalid input:{ string: \"042e5523ff6b250L\"\n"
            + " } for bytes column:{ parent_span_id }");
  }

  @Test
  public void testQueryWithBytesColumnWithNullId() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id").build());

    Filter parentIdFilter = createNotEqualsFilter("Span.attributes.parent_span_id", "null");
    Filter andFilter =
        Filter.newBuilder().setOperator(Operator.AND).addChildFilter(parentIdFilter).build();
    builder.setFilter(andFilter);
    builder.setLimit(5);

    QueryRequest request = builder.build();
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        request,
        "SELECT span_id FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND ( parent_span_id != '' ) limit 5",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithBytesColumnWithEmptyId() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id").build());

    Filter parentIdFilter = createNotEqualsFilter("Span.attributes.parent_span_id", "''");
    Filter andFilter =
        Filter.newBuilder().setOperator(Operator.AND).addChildFilter(parentIdFilter).build();
    builder.setFilter(andFilter);
    builder.setLimit(5);

    QueryRequest request = builder.build();
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        request,
        "SELECT span_id FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND ( parent_span_id != '' ) limit 5",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithBytesColumnInFilter() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.metrics.duration_millis"));

    // Though span id is bytes in Pinot, top layers send the value as hex string.
    builder.setFilter(createInFilter("Span.id", List.of("042e5523ff6b2506")));

    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        builder.build(),
        "SELECT duration_millis FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND span_id IN ('042e5523ff6b2506')",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithStringColumnWithNullString() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id"));

    Filter parentIdFilter = createNotEqualsFilter("Span.id", "null");
    Filter andFilter =
        Filter.newBuilder().setOperator(Operator.AND).addChildFilter(parentIdFilter).build();
    builder.setFilter(andFilter);
    builder.setLimit(5);

    QueryRequest request = builder.build();
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        request,
        "SELECT span_id FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND ( span_id != '' ) limit 5",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithLongColumn() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id"));

    Expression durationColumn = createColumnExpression("Span.metrics.duration_millis").build();
    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.GE)
            .setLhs(durationColumn)
            .setRhs(createLongLiteralValueExpression(1000))
            .build();
    builder.setFilter(andFilter);
    builder.setLimit(5);

    QueryRequest request = builder.build();
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        request,
        "SELECT span_id FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND duration_millis >= 1000 limit 5",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithLongColumnWithLikeFilter() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id"));

    Expression durationColumn = createColumnExpression("Span.metrics.duration_millis").build();
    Filter likeFilter =
        Filter.newBuilder()
            .setOperator(Operator.LIKE)
            .setLhs(durationColumn)
            .setRhs(createLongLiteralValueExpression(5000))
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
            + "AND REGEXP_LIKE(duration_millis,5000)",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithPercentileAggregation() {
    Filter startTimeFilter =
        createTimeFilter("Span.start_time_millis", Operator.GT, 1570658506605L);
    Filter endTimeFilter = createTimeFilter("Span.end_time_millis", Operator.LT, 1570744906673L);
    Expression percentileAgg =
        createAliasedFunctionExpression(
                "PERCENTILE99", "Span.metrics.duration_millis", "P99_duration")
            .build();

    QueryRequest queryRequest =
        QueryRequest.newBuilder()
            .addAggregation(percentileAgg)
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
        "select PERCENTILETDIGEST99(duration_millis) from SpanEventView"
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
  public void testQueryWithNulls() {
    Expression conditionalString =
        Expression.newBuilder()
            .setFunction(
                Function.newBuilder()
                    .setFunctionName(QueryFunctionConstants.QUERY_FUNCTION_CONDITIONAL)
                    .addArguments(createStringLiteralValueExpression("true"))
                    .addArguments(createColumnExpression("Span.id"))
                    .addArguments(createNullStringLiteralValueExpression()))
            .build();

    Expression conditionalNumber =
        Expression.newBuilder()
            .setFunction(
                Function.newBuilder()
                    .setFunctionName(QueryFunctionConstants.QUERY_FUNCTION_CONDITIONAL)
                    .addArguments(createStringLiteralValueExpression("true"))
                    .addArguments(createColumnExpression("Span.metrics.duration_millis"))
                    .addArguments(createNullNumberLiteralValueExpression()))
            .build();

    QueryRequest queryRequest =
        QueryRequest.newBuilder()
            .addSelection(conditionalString)
            .addSelection(conditionalNumber)
            .setLimit(15)
            .build();

    ViewDefinition viewDefinition = getDefaultViewDefinition();
    defaultMockingForExecutionContext();

    assertPQLQuery(
        queryRequest,
        "select conditional('true',span_id,'null'), conditional('true',duration_millis,0)"
            + " from SpanEventView"
            + " where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' limit 15",
        viewDefinition,
        executionContext);
  }

  @Test
  public void testQueryWithAverageRateInOrderBy() {
    ViewDefinition viewDefinition = getServiceViewDefinition();
    defaultMockingForExecutionContext();
    when(executionContext.getTimeRangeDuration()).thenReturn(Optional.of(Duration.ofMinutes(60)));

    assertPQLQuery(
        buildAvgRateQueryForOrderBy(),
        "select service_id, service_name, count(*) FROM RawServiceView WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "and ( start_time_millis >= 1637297304041 and start_time_millis < 1637300904041 and service_id != 'null' ) "
            + "group by service_id, service_name "
            + "order by sum(div(error_count, 3600.0)) "
            + "limit 10000",
        viewDefinition,
        executionContext);
  }

  private QueryRequest buildSimpleQueryWithFilter(Filter filter) {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id").build());
    builder.setFilter(filter);
    return builder.build();
  }

  private QueryRequest buildAvgRateQueryForOrderBy() {
    Builder builder = QueryRequest.newBuilder();

    Expression serviceId = createColumnExpression("SERVICE.id").build();
    Expression serviceName = createColumnExpression("SERVICE.name").build();
    Expression serviceErrorCount = createColumnExpression("SERVICE.errorCount").build();

    Expression countFunction = createFunctionExpression("COUNT", serviceId);
    Expression avgrateFunction = createFunctionExpression("AVGRATE", serviceErrorCount);

    Filter nullCheckFilter = createNullStringFilter("SERVICE.id", Operator.NEQ);
    Filter startTimeFilter = createTimeFilter("SERVICE.startTime", Operator.GE, 1637297304041L);
    Filter endTimeFilter = createTimeFilter("SERVICE.startTime", Operator.LT, 1637300904041L);
    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .addChildFilter(nullCheckFilter)
            .build();
    builder.setFilter(andFilter);

    builder.addSelection(serviceId);
    builder.addSelection(serviceName);
    builder.addSelection(countFunction);

    builder.addGroupBy(serviceId);
    builder.addGroupBy(serviceName);

    builder.addOrderBy(createOrderByExpression(avgrateFunction.toBuilder(), SortOrder.ASC));

    builder.setLimit(10000);
    return builder.build();
  }

  private QueryRequest buildOrderByQuery() {
    Builder builder = QueryRequest.newBuilder();
    Expression startTimeColumn = createColumnExpression("Span.start_time_millis").build();
    Expression endTimeColumn = createColumnExpression("Span.end_time_millis").build();

    builder.addSelection(createColumnExpression("Span.id"));
    builder.addSelection(startTimeColumn);
    builder.addSelection(endTimeColumn);

    builder.addOrderBy(createOrderByExpression(startTimeColumn.toBuilder(), SortOrder.DESC));
    builder.addOrderBy(createOrderByExpression(endTimeColumn.toBuilder(), SortOrder.ASC));

    builder.setLimit(100);
    return builder.build();
  }

  private QueryRequest buildMultipleGroupByMultipleAggQuery() {
    Builder builder = QueryRequest.newBuilder();
    builder.addAggregation(createCountByColumnSelection("Span.id"));
    Expression avg =
        createFunctionExpression("AVG", createColumnExpression("Span.duration_millis").build());
    builder.addAggregation(avg);

    Filter startTimeFilter =
        createTimeFilter("Span.start_time_millis", Operator.GT, 1570658506605L);
    Filter endTimeFilter = createTimeFilter("Span.end_time_millis", Operator.LT, 1570744906673L);

    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .build();
    builder.setFilter(andFilter);

    builder.addGroupBy(createColumnExpression("Span.serviceName"));
    builder.addGroupBy(createColumnExpression("Span.displaySpanName"));
    return builder.build();
  }

  private QueryRequest buildMultipleGroupByMultipleAggAndOrderByQuery() {
    Builder builder = QueryRequest.newBuilder();
    builder.addAggregation(createCountByColumnSelection("Span.id"));
    Expression avg =
        createFunctionExpression("AVG", createColumnExpression("Span.duration_millis").build());
    builder.addAggregation(avg);

    Filter startTimeFilter =
        createTimeFilter("Span.start_time_millis", Operator.GT, 1570658506605L);
    Filter endTimeFilter = createTimeFilter("Span.end_time_millis", Operator.LT, 1570744906673L);

    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .build();
    builder.setFilter(andFilter);

    builder.addGroupBy(createColumnExpression("Span.serviceName"));
    builder.addGroupBy(createColumnExpression("Span.displaySpanName"));

    builder.addOrderBy(
        createOrderByExpression(createColumnExpression("Span.serviceName"), SortOrder.ASC));
    builder.addOrderBy(
        createOrderByExpression(
            createAliasedFunctionExpression("AVG", "Span.duration_millis", "avg_duration_millis"),
            SortOrder.DESC));
    builder.addOrderBy(
        createOrderByExpression(
            createAliasedFunctionExpression("COUNT", "Span.id", "count_span_id"), SortOrder.DESC));
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

  private void assertExceptionOnPQLQuery(
      QueryRequest queryRequest,
      Class<? extends Throwable> exceptionClass,
      String expectedMessage) {

    QueryRequestToPinotSQLConverter converter =
        new QueryRequestToPinotSQLConverter(
            getDefaultViewDefinition(), new PinotFunctionConverter());

    Throwable exception =
        Assertions.assertThrows(
            exceptionClass,
            () ->
                converter.toSQL(
                    new ExecutionContext("__default", queryRequest),
                    queryRequest,
                    createSelectionsFromQueryRequest(queryRequest)));

    String actualMessage = exception.getMessage();
    Assertions.assertTrue(actualMessage.contains(expectedMessage));
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

  private ViewDefinition getServiceViewDefinition() {
    Config serviceFileConfig =
        ConfigFactory.parseURL(
            requireNonNull(
                QueryRequestToPinotSQLConverterTest.class
                    .getClassLoader()
                    .getResource(TEST_SERVICE_REQUEST_HANDLER_CONFIG_FILE)));

    return ViewDefinition.parse(
        serviceFileConfig.getConfig("requestHandlerInfo.viewDefinition"), TENANT_COLUMN_NAME);
  }

  private void defaultMockingForExecutionContext() {
    when(executionContext.getTenantId()).thenReturn("__default");
  }
}
