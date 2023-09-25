package org.hypertrace.core.query.service.trino;

import static java.util.Objects.requireNonNull;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createAliasedFunctionExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createColumnExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createComplexAttributeExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createCompositeFilter;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createCountByColumnSelection;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createEqualsFilter;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createFunctionExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createInFilter;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createLongLiteralValueExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createNotEqualsFilter;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createNotInFilter;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createNullNumberLiteralValueExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createNullStringFilter;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createNullStringLiteralValueExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createOrderByExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createStringArrayLiteralValueExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createTimeFilter;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createTimestampFilter;
import static org.hypertrace.core.query.service.QueryRequestUtil.createContainsKeyFilter;
import static org.hypertrace.core.query.service.QueryRequestUtil.createNotContainsKeyFilter;
import static org.hypertrace.core.query.service.QueryRequestUtil.createStringLiteralValueExpression;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.QueryFunctionConstants;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.QueryRequest.Builder;
import org.hypertrace.core.query.service.api.SortOrder;
import org.hypertrace.core.query.service.trino.converters.TrinoFunctionConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class QueryRequestToTrinoSQLConverterTest {

  private static final String TENANT_ID = "3e761879-c77b-4d8f-a075-62ff28e8fa8a";
  private static final String TENANT_COLUMN_NAME = "customer_id";

  private static final String TEST_REQUEST_HANDLER_CONFIG_FILE = "trino_request_handler.conf";

  private ExecutionContext executionContext;

  @BeforeEach
  void setup() {
    executionContext = mock(ExecutionContext.class);
  }

  @Test
  void testQuery() {
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

    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        builder.build(),
        "select lower(to_hex(span_id)), tags, request_headers "
            + "FROM span-event-view "
            + "where "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "and ( start_time_millis > 1557780911508 and end_time_millis < 1557780938419 )",
        tableDefinition,
        executionContext);
  }

  @Test
  void testQueryWithoutFilter() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id").build());
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        builder.build(),
        "Select lower(to_hex(span_id)) FROM span-event-view "
            + "where "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "'",
        tableDefinition,
        executionContext);
  }

  @Test
  void testQuerySingleDistinctSelection() {
    Builder builder = QueryRequest.newBuilder();
    builder.setDistinctSelections(true).addSelection(createColumnExpression("Span.id"));
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        builder.build(),
        "Select distinct lower(to_hex(span_id)) FROM span-event-view "
            + "where "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "'",
        tableDefinition,
        executionContext);
  }

  @Test
  void testQueryMultipleDistinctSelection() {
    Builder builder = QueryRequest.newBuilder();
    builder
        .setDistinctSelections(true)
        .addSelection(createColumnExpression("Span.id"))
        .addSelection(createColumnExpression("Span.displaySpanName"))
        .addSelection(createColumnExpression("Span.serviceName"));
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        builder.build(),
        "Select distinct lower(to_hex(span_id)), span_name, service_name FROM span-event-view "
            + "where "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "'",
        tableDefinition,
        executionContext);
  }

  @Test
  void testQueryWithStringFilter() {
    QueryRequest queryRequest =
        buildSimpleQueryWithFilter(createEqualsFilter("Span.displaySpanName", "GET /login"));
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        queryRequest,
        "Select lower(to_hex(span_id)) FROM span-event-view "
            + "WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND span_name = 'GET /login'",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testSQLiWithStringValueFilter() {
    QueryRequest queryRequest =
        buildSimpleQueryWithFilter(
            createEqualsFilter("Span.displaySpanName", "GET /login' OR tenant_id = 'tenant2"));
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        queryRequest,
        "Select encode(span_id, 'hex') FROM public.\"span-event-view\" WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND span_name = 'GET /login'' OR tenant_id = ''tenant2'",
        tableDefinition,
        executionContext);
  }

  @Test
  void testQueryWithBooleanFilter() {
    Filter isEntryFilter = createEqualsFilter("Span.is_entry", true);
    Filter isBareFilter = createEqualsFilter("Span.isBare", false);
    QueryRequest queryRequest =
        buildSimpleQueryWithFilter(
            createCompositeFilter(Operator.AND, isEntryFilter, isBareFilter).build());
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        queryRequest,
        "Select lower(to_hex(span_id)) FROM span-event-view WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND ( is_entry = true AND is_bare = false )",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithDoubleFilter() {
    QueryRequest queryRequest =
        buildSimpleQueryWithFilter(createEqualsFilter("Span.metrics.duration_millis", 1.2));
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        queryRequest,
        "Select encode(span_id, 'hex') FROM public.\"span-event-view\" WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND duration_millis = 1.2",
        tableDefinition,
        executionContext);
  }

  @Test
  void testQueryWithFloatFilter() {
    QueryRequest queryRequest =
        buildSimpleQueryWithFilter(createEqualsFilter("Span.user_latitude", 45.8234));
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        queryRequest,
        "Select lower(to_hex(span_id)) FROM span-event-view WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND user_latitude = 45.8234",
        tableDefinition,
        executionContext);
  }

  @Test
  void testQueryWithIntFilter() {
    QueryRequest queryRequest =
        buildSimpleQueryWithFilter(createEqualsFilter("Span.metrics.duration_millis", 100));
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        queryRequest,
        "Select lower(to_hex(span_id)) FROM span-event-view WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND duration_millis = 100",
        tableDefinition,
        executionContext);
  }

  @Test
  void testQueryWithTimestampFilter() {
    QueryRequest queryRequest =
        buildSimpleQueryWithFilter(
            createTimestampFilter("Span.start_time_millis", Operator.EQ, 123456));
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        queryRequest,
        "Select lower(to_hex(span_id)) FROM span-event-view WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND start_time_millis = 123456",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithOrderBy() {
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        buildOrderByQuery(),
        "Select encode(span_id, 'hex'), start_time_millis, end_time_millis FROM public.\"span-event-view\" WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "order by 2 desc, 3 limit 100",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithOrderByWithPagination() {
    QueryRequest orderByQueryRequest = buildOrderByQuery();
    Builder builder = QueryRequest.newBuilder(orderByQueryRequest);
    builder.setOffset(1000);
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        builder.build(),
        "Select encode(span_id, 'hex'), start_time_millis, end_time_millis FROM public.\"span-event-view\" WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "order by 2 desc, 3 offset 1000 limit 100",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithGroupByWithMultipleAggregates() {
    QueryRequest groupByQueryRequest = buildMultipleGroupByMultipleAggQuery();
    Builder builder = QueryRequest.newBuilder(groupByQueryRequest);
    builder.setLimit(20);
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        builder.build(),
        "select count(*), avg(duration_millis) FROM public.\"span-event-view\""
            + " where "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "and ( start_time_millis > 1570658506605 and end_time_millis < 1570744906673 )"
            + " group by service_name, span_name limit 20",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithGroupByWithMultipleAggregatesAndOrderBy() {
    QueryRequest orderByQueryRequest = buildMultipleGroupByMultipleAggAndOrderByQuery();
    Builder builder = QueryRequest.newBuilder(orderByQueryRequest);
    builder.setLimit(20);
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        builder.build(),
        "select count(*), avg(duration_millis) FROM public.\"span-event-view\""
            + " where "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "and ( start_time_millis > 1570658506605 and end_time_millis < 1570744906673 )"
            + " group by service_name, span_name order by service_name, 2 desc, 1 desc limit 20",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithDistinctCountAggregation() {
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

    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        queryRequest,
        "select count(distinct encode(span_id, 'hex')) FROM public.\"span-event-view\""
            + " where "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "and ( start_time_millis > 1570658506605 and end_time_millis < 1570744906673 )"
            + " limit 15",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithDistinctCountAggregationAndGroupBy() {
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

    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        queryRequest,
        "select encode(span_id, 'hex'), count(distinct encode(span_id, 'hex')) FROM public.\"span-event-view\""
            + " where "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "and ( start_time_millis > 1570658506605 and end_time_millis < 1570744906673 )"
            + " group by span_id order by count(distinct span_id) limit 15",
        tableDefinition,
        executionContext);
  }

  @Test
  void testQueryWithStringArray() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id").build());

    String spanId1 = "042e5523ff6b2506";
    String spanId2 = "041e5523ff6b2501";
    Filter filter = createInFilter("Span.id", List.of(spanId1, spanId2));
    builder.setFilter(filter);

    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        builder.build(),
        "SELECT lower(to_hex(span_id)) FROM span-event-view "
            + "WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND lower(to_hex(span_id)) IN "
            + "('"
            + spanId1
            + "', '"
            + spanId2
            + "')",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testSQLiWithStringArrayFilter() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.displaySpanName"));

    Filter filter =
        createInFilter(
            "Span.displaySpanName", List.of("1') OR tenant_id = 'tenant2' and span_name IN ('1"));
    builder.setFilter(filter);

    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        builder.build(),
        "SELECT span_name FROM public.\"span-event-view\" WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND span_name IN ('1'') OR tenant_id = ''tenant2'' and span_name IN (''1')",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithLikeOperator() {
    Builder builder = QueryRequest.newBuilder();
    Expression spanId = createColumnExpression("Span.displaySpanName").build();
    builder.addSelection(spanId);

    Filter likeFilter =
        Filter.newBuilder()
            .setOperator(Operator.LIKE)
            .setLhs(spanId)
            .setRhs(createStringLiteralValueExpression("%test%"))
            .build();
    builder.setFilter(likeFilter);

    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        builder.build(),
        "SELECT span_name FROM public.\"span-event-view\" "
            + "WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND span_name ~* '%test%'",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithContainsKeyOperator() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.tags"));
    builder.setFilter(createContainsKeyFilter("Span.tags", "FLAGS"));

    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        builder.build(),
        "SELECT cast(tags as text) FROM public.\"span-event-view\" "
            + "WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND tags->>'flags' IS NOT NULL",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithNotContainsKeyOperator() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.tags"));
    builder.setFilter(createNotContainsKeyFilter("Span.tags", "FLAGS"));

    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        builder.build(),
        "SELECT cast(tags as text) FROM public.\"span-event-view\" "
            + "WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND tags->>'flags' IS NULL",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithContainsKeyValueOperator() {
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

    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        builder.build(),
        "SELECT cast(tags as text) FROM public.\"span-event-view\" "
            + "WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND tags->>'flags' = '0'",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithContainsKeyLikeOperator() {
    Builder builder = QueryRequest.newBuilder();
    Expression spanTag = createColumnExpression("Span.tags").build();
    builder.addSelection(spanTag);

    Expression tag = createStringLiteralValueExpression("my_tag_name.*");
    Filter likeFilter =
        Filter.newBuilder()
            .setOperator(Operator.CONTAINS_KEY_LIKE)
            .setLhs(spanTag)
            .setRhs(tag)
            .build();
    builder.setFilter(likeFilter);

    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        builder.build(),
        "SELECT cast(tags as text) FROM public.\"span-event-view\" "
            + "WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND tags::jsonb::text ~* '.*\"my_tag_name.*\":.*'",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithComplexKeyValueOperator() {
    Builder builder = QueryRequest.newBuilder();
    Expression spanTag = createColumnExpression("Span.tags").build();
    builder.addSelection(spanTag);

    Expression spanTags = createComplexAttributeExpression("Span.tags", "FLAGS").build();
    Filter filter =
        Filter.newBuilder()
            .setLhs(spanTags)
            .setOperator(Operator.EQ)
            .setRhs(createStringLiteralValueExpression("0"))
            .build();
    builder.setFilter(filter);

    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        builder.build(),
        "SELECT cast(tags as text) FROM public.\"span-event-view\" "
            + "WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND tags->>'flags' = '0'",
        tableDefinition,
        executionContext);
  }

  @Test
  void testQueryWithBytesColumnWithValidId() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id").build());

    Filter parentIdFilter =
        createEqualsFilter("Span.attributes.parent_span_id", "042e5523ff6b2506");
    Filter andFilter =
        Filter.newBuilder().setOperator(Operator.AND).addChildFilter(parentIdFilter).build();
    builder.setFilter(andFilter);
    builder.setLimit(5);

    QueryRequest request = builder.build();
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        request,
        "SELECT lower(to_hex(span_id)) FROM span-event-view "
            + "WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND ( lower(to_hex(parent_span_id)) = '042e5523ff6b2506' ) limit 5",
        tableDefinition,
        executionContext);
  }

  @Test
  void testQueryWithBytesColumnWithInValidId() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id").build());

    Filter parentIdFilter =
        createEqualsFilter("Span.attributes.parent_span_id", "042e5523ff6b250L");
    Filter andFilter =
        Filter.newBuilder().setOperator(Operator.AND).addChildFilter(parentIdFilter).build();
    builder.setFilter(andFilter);
    builder.setLimit(5);

    assertExceptionOnSQLQuery(
        builder.build(),
        IllegalArgumentException.class,
        "Invalid input:{ 042e5523ff6b250L"
            + " } for bytes column:{ lower(to_hex(parent_span_id)) }");
  }

  @Test
  void testQueryWithBytesColumnWithNullId() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id").build());

    Filter parentIdFilter = createNotEqualsFilter("Span.attributes.parent_span_id", "null");
    Filter andFilter =
        Filter.newBuilder().setOperator(Operator.AND).addChildFilter(parentIdFilter).build();
    builder.setFilter(andFilter);
    builder.setLimit(5);

    QueryRequest request = builder.build();
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        request,
        "SELECT lower(to_hex(span_id)) FROM span-event-view "
            + "WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND ( lower(to_hex(parent_span_id)) != '' ) limit 5",
        tableDefinition,
        executionContext);
  }

  @Test
  void testQueryWithBytesColumnWithEmptyId() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id").build());

    Filter parentIdFilter = createNotEqualsFilter("Span.attributes.parent_span_id", "''");
    Filter andFilter =
        Filter.newBuilder().setOperator(Operator.AND).addChildFilter(parentIdFilter).build();
    builder.setFilter(andFilter);
    builder.setLimit(5);

    QueryRequest request = builder.build();
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        request,
        "SELECT lower(to_hex(span_id)) FROM span-event-view "
            + "WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND ( lower(to_hex(parent_span_id)) != '' ) limit 5",
        tableDefinition,
        executionContext);
  }

  @Test
  void testQueryWithBytesColumnInFilter() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.metrics.duration_millis"));

    // Though span id is bytes in Postgres, top layers send the value as hex string.
    builder.setFilter(createInFilter("Span.id", List.of("042e5523ff6b2506")));

    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        builder.build(),
        "SELECT duration_millis FROM span-event-view "
            + "WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND lower(to_hex(span_id)) in ('042e5523ff6b2506')",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithArrayColumnWithValidValue() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id").build());

    Filter filter = createEqualsFilter("Span.labels", "label1");
    builder.setFilter(filter);
    builder.setLimit(5);

    QueryRequest request = builder.build();
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        request,
        "SELECT encode(span_id, 'hex') FROM public.\"span-event-view\" "
            + "WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND labels && '{label1}' limit 5",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithArrayColumnWithNullValue() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id").build());

    Filter filter = createNotEqualsFilter("Span.labels", "null");
    builder.setFilter(filter);
    builder.setLimit(5);

    QueryRequest request = builder.build();
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        request,
        "SELECT encode(span_id, 'hex') FROM public.\"span-event-view\" "
            + "WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND labels != '{}' limit 5",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithArrayColumnWithEmptyValue() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id").build());

    Filter filter = createNotEqualsFilter("Span.labels", "''");
    builder.setFilter(filter);
    builder.setLimit(5);

    QueryRequest request = builder.build();
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        request,
        "SELECT encode(span_id, 'hex') FROM public.\"span-event-view\" "
            + "WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND labels != '{}' limit 5",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithArrayColumnNotEqualsFilter() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id").build());

    Filter filter = createNotEqualsFilter("Span.labels", "label1");
    builder.setFilter(filter);
    builder.setLimit(5);

    QueryRequest request = builder.build();
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        request,
        "SELECT encode(span_id, 'hex') FROM public.\"span-event-view\" "
            + "WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND NOT labels && '{label1}' limit 5",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithArrayColumnInFilter() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id").build());

    Filter filter = createInFilter("Span.labels", List.of("label1", "label2"));
    builder.setFilter(filter);
    builder.setLimit(5);

    QueryRequest request = builder.build();
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        request,
        "SELECT encode(span_id, 'hex') FROM public.\"span-event-view\" "
            + "WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND labels && '{label1, label2}' limit 5",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithArrayColumnNotInFilter() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id").build());

    Filter filter = createNotInFilter("Span.labels", List.of("label1", "label2"));
    builder.setFilter(filter);
    builder.setLimit(5);

    QueryRequest request = builder.build();
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        request,
        "SELECT encode(span_id, 'hex') FROM public.\"span-event-view\" "
            + "WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND NOT labels && '{label1, label2}' limit 5",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithStringColumnWithNullString() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(createColumnExpression("Span.id"));

    Filter parentIdFilter = createNotEqualsFilter("Span.id", "null");
    Filter andFilter =
        Filter.newBuilder().setOperator(Operator.AND).addChildFilter(parentIdFilter).build();
    builder.setFilter(andFilter);
    builder.setLimit(5);

    QueryRequest request = builder.build();
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        request,
        "SELECT encode(span_id, 'hex') FROM public.\"span-event-view\" "
            + "WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND ( span_id != '' ) limit 5",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithLongColumn() {
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
    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        request,
        "SELECT encode(span_id, 'hex') FROM public.\"span-event-view\" "
            + "WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND duration_millis >= 1000 limit 5",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithLongColumnWithLikeFilter() {
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

    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        builder.build(),
        "SELECT encode(span_id, 'hex') FROM public.\"span-event-view\" "
            + "WHERE "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND duration_millis ~* 5000",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithPercentileAggregation() {
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

    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        queryRequest,
        "select percentile_cont(0.990000) within group (order by (duration_millis) asc)"
            + " FROM public.\"span-event-view\""
            + " where "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "and ( start_time_millis > 1570658506605 and end_time_millis < 1570744906673 )"
            + " limit 15",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithNulls() {
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

    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        queryRequest,
        "select conditional('true',encode(span_id, 'hex'),'null'), conditional('true',duration_millis,0)"
            + " from public.\"span-event-view\""
            + " where "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' limit 15",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithDistinctCountAggregationAndGroupByForArrayColumn() {
    Filter startTimeFilter =
        createTimeFilter("Span.start_time_millis", Operator.GT, 1570658506605L);
    Filter endTimeFilter = createTimeFilter("Span.end_time_millis", Operator.LT, 1570744906673L);
    QueryRequest queryRequest =
        QueryRequest.newBuilder()
            .addGroupBy(createColumnExpression("Span.id"))
            .addAggregation(
                createAliasedFunctionExpression(
                    "DISTINCTCOUNT", "Span.labels", "distinctcount_labels"))
            .addAggregation(
                createAliasedFunctionExpression(
                    "AVG", "Span.duration_millis", "avg_duration_millis"))
            .addAggregation(
                createAliasedFunctionExpression(
                    "MAX", "Span.duration_millis", "avg_duration_millis"))
            .setFilter(
                Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addChildFilter(startTimeFilter)
                    .addChildFilter(endTimeFilter)
                    .build())
            .addOrderBy(
                createOrderByExpression(
                    createAliasedFunctionExpression(
                        "DISTINCTCOUNT", "Span.labels", "distinctcount_labels"),
                    SortOrder.ASC))
            .setLimit(15)
            .build();

    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        queryRequest,
        "select count(distinct column1),"
            + " avg(duration_millis), max(duration_millis) FROM"
            + " ( select unnest(labels) as column1, duration_millis, span_id from public.\"span-event-view\""
            + " where "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "and ( start_time_millis > 1570658506605 and end_time_millis < 1570744906673 )"
            + " ) as intermediate_table"
            + " group by span_id order by 1 limit 15",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithPercentileForTdigestColumn() {
    Filter startTimeFilter =
        createTimeFilter("Span.start_time_millis", Operator.GT, 1570658506605L);
    Filter endTimeFilter = createTimeFilter("Span.end_time_millis", Operator.LT, 1570744906673L);
    QueryRequest queryRequest =
        QueryRequest.newBuilder()
            .addSelection(createColumnExpression("Span.id"))
            .addGroupBy(createColumnExpression("Span.id"))
            .addAggregation(
                createAliasedFunctionExpression(
                    "PERCENTILE99", "Span.response_time_millis", "P99_response_time"))
            .setFilter(
                Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addChildFilter(startTimeFilter)
                    .addChildFilter(endTimeFilter)
                    .build())
            .setLimit(15)
            .build();

    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        queryRequest,
        "select encode(span_id, 'hex'), tdigest_percentile(response_time_millis_tdigest,0.99)"
            + " from public.\"span-event-view\""
            + " where "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "and ( start_time_millis > 1570658506605 and end_time_millis < 1570744906673 )"
            + " group by span_id limit 15",
        tableDefinition,
        executionContext);
  }

  // @Test
  void testQueryWithAvgForTdigestColumn() {
    Filter startTimeFilter =
        createTimeFilter("Span.start_time_millis", Operator.GT, 1570658506605L);
    Filter endTimeFilter = createTimeFilter("Span.end_time_millis", Operator.LT, 1570744906673L);
    QueryRequest queryRequest =
        QueryRequest.newBuilder()
            .addSelection(createColumnExpression("Span.id"))
            .addGroupBy(createColumnExpression("Span.id"))
            .addAggregation(
                createAliasedFunctionExpression(
                    "AVG", "Span.response_time_millis", "P99_response_time"))
            .setFilter(
                Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addChildFilter(startTimeFilter)
                    .addChildFilter(endTimeFilter)
                    .build())
            .setLimit(15)
            .build();

    TableDefinition tableDefinition = getDefaultTableDefinition();
    defaultMockingForExecutionContext();

    assertSQLQuery(
        queryRequest,
        "select encode(span_id, 'hex'), tdigest_avg(response_time_millis_tdigest,0.001,0.999)"
            + " from public.\"span-event-view\""
            + " where "
            + tableDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "and ( start_time_millis > 1570658506605 and end_time_millis < 1570744906673 )"
            + " group by span_id limit 15",
        tableDefinition,
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
            createAliasedFunctionExpression("COUNT", "Span.id", "count_encode(span_id, 'hex')"),
            SortOrder.DESC));
    return builder.build();
  }

  private void assertSQLQuery(
      QueryRequest queryRequest,
      String expectedQuery,
      TableDefinition tableDefinition,
      ExecutionContext executionContext) {
    QueryRequestToTrinoSQLConverter converter =
        new QueryRequestToTrinoSQLConverter(
            tableDefinition, new TrinoFunctionConverter(tableDefinition));
    Entry<String, Params> statementToParam =
        converter.toSQL(
            executionContext, queryRequest, createSelectionsFromQueryRequest(queryRequest));
    String resolvedStatement =
        converter.resolveStatement(statementToParam.getKey(), statementToParam.getValue());
    Assertions.assertEquals(expectedQuery.toLowerCase(), resolvedStatement.toLowerCase());
  }

  private void assertExceptionOnSQLQuery(
      QueryRequest queryRequest,
      Class<? extends Throwable> exceptionClass,
      String expectedMessage) {

    QueryRequestToTrinoSQLConverter converter =
        new QueryRequestToTrinoSQLConverter(
            getDefaultTableDefinition(), new TrinoFunctionConverter(getDefaultTableDefinition()));

    Throwable exception =
        Assertions.assertThrows(
            exceptionClass,
            () ->
                converter.toSQL(
                    new ExecutionContext(TENANT_ID, queryRequest),
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

    selections.addAll(queryRequest.getSelectionList());
    selections.addAll(queryRequest.getAggregationList());

    return selections;
  }

  private TableDefinition getDefaultTableDefinition() {
    Config fileConfig =
        ConfigFactory.parseURL(
            requireNonNull(
                QueryRequestToTrinoSQLConverterTest.class
                    .getClassLoader()
                    .getResource(TEST_REQUEST_HANDLER_CONFIG_FILE)));

    return TableDefinition.parse(
        fileConfig.getConfig("requestHandlerInfo.tableDefinition"),
        TENANT_COLUMN_NAME,
        Optional.empty());
  }

  private void defaultMockingForExecutionContext() {
    when(executionContext.getTenantId()).thenReturn(TENANT_ID);
  }
}
