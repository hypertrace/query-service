package org.hypertrace.core.query.service.pinot;

import static java.util.Objects.requireNonNull;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createColumnExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createFunctionExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createOrderByExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createStringLiteralValueExpression;
import static org.hypertrace.core.query.service.QueryRequestUtil.createNullNumberLiteralExpression;
import static org.hypertrace.core.query.service.QueryRequestUtil.createNullStringLiteralExpression;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.Request;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.QueryFunctionConstants;
import org.hypertrace.core.query.service.QueryRequestBuilderUtils;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.QueryRequest.Builder;
import org.hypertrace.core.query.service.api.SortOrder;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;
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

  @BeforeEach
  public void setup() {
    connection = mock(Connection.class);
    Mockito.when(connection.prepareStatement(any(Request.class))).thenCallRealMethod();
  }

  @Test
  public void testQuery() {
    Builder builder = QueryRequest.newBuilder();
    ColumnIdentifier spanId = ColumnIdentifier.newBuilder().setColumnName("Span.id").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(spanId).build());

    ColumnIdentifier tags = ColumnIdentifier.newBuilder().setColumnName("Span.tags").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(tags).build());

    ColumnIdentifier request_headers =
        ColumnIdentifier.newBuilder().setColumnName("Span.attributes.request_headers").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(request_headers).build());

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

    assertPQLQuery(
        builder.build(),
        "select span_id, tags__keys, tags__values, request_headers__keys, request_headers__values "
            + "from SpanEventView "
            + "where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "and ( start_time_millis > '1557780911508' and end_time_millis < '1557780938419' )",
        viewDefinition);
  }

  @Test
  public void testQueryWithoutFilter() {
    Builder builder = QueryRequest.newBuilder();
    ColumnIdentifier spanId = ColumnIdentifier.newBuilder().setColumnName("Span.id").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(spanId).build());
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    assertPQLQuery(
        builder.build(),
        "Select span_id FROM SpanEventView "
            + "where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "'",
        viewDefinition);
  }

  @Test
  public void testQuerySingleDistinctSelection() {
    Builder builder = QueryRequest.newBuilder();
    builder.setDistinctSelections(true).addSelection(createColumnExpression("Span.id"));
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    assertPQLQuery(
        builder.build(),
        "Select distinct span_id FROM SpanEventView "
            + "where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "'",
        viewDefinition);
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
    assertPQLQuery(
        builder.build(),
        "Select distinct span_id, span_name, service_name FROM SpanEventView "
            + "where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "'",
        viewDefinition);
  }

  @Test
  public void testQueryWithStringFilter() {
    QueryRequest queryRequest =
        buildSimpleQueryWithFilter(
            createStringFilter("Span.displaySpanName", Operator.EQ, "GET /login"));
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    assertPQLQuery(
        queryRequest,
        "Select span_id FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND span_name = 'GET /login'",
        viewDefinition);
  }

  @Test
  public void testSQLiWithStringValueFilter() {
    QueryRequest queryRequest =
        buildSimpleQueryWithFilter(
            createStringFilter(
                "Span.displaySpanName", Operator.EQ, "GET /login' OR tenant_id = 'tenant2"));
    ViewDefinition viewDefinition = getDefaultViewDefinition();

    assertPQLQuery(
        queryRequest,
        "Select span_id FROM SpanEventView WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND span_name = 'GET /login'' OR tenant_id = ''tenant2'",
        viewDefinition);
  }

  @Test
  public void testQueryWithBooleanFilter() {
    QueryRequest queryRequest =
        buildSimpleQueryWithFilter(createBooleanFilter("Span.is_entry", Operator.EQ, true));
    ViewDefinition viewDefinition = getDefaultViewDefinition();

    assertPQLQuery(
        queryRequest,
        "Select span_id FROM SpanEventView WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND is_entry = 'true'",
        viewDefinition);
  }

  @Test
  public void testQueryWithDoubleFilter() {
    QueryRequest queryRequest =
        buildSimpleQueryWithFilter(
            createDoubleFilter("Span.metrics.duration_millis", Operator.EQ, 1.2));
    ViewDefinition viewDefinition = getDefaultViewDefinition();

    assertPQLQuery(
        queryRequest,
        "Select span_id FROM SpanEventView WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND duration_millis = 1.2",
        viewDefinition);
  }

  @Test
  public void testQueryWithFloatFilter() {
    QueryRequest queryRequest =
        buildSimpleQueryWithFilter(
            createFloatFilter("Span.metrics.duration_millis", Operator.EQ, 1.2f));
    ViewDefinition viewDefinition = getDefaultViewDefinition();

    assertPQLQuery(
        queryRequest,
        "Select span_id FROM SpanEventView WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND duration_millis = 1.2",
        viewDefinition);
  }

  @Test
  public void testQueryWithIntFilter() {
    QueryRequest queryRequest =
        buildSimpleQueryWithFilter(createIntFilter("Span.metrics.duration_millis", Operator.EQ, 1));
    ViewDefinition viewDefinition = getDefaultViewDefinition();

    assertPQLQuery(
        queryRequest,
        "Select span_id FROM SpanEventView WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND duration_millis = 1",
        viewDefinition);
  }

  @Test
  public void testQueryWithTimestampFilter() {
    QueryRequest queryRequest =
        buildSimpleQueryWithFilter(createTimestampFilter("Span.is_entry", Operator.EQ, 123456));
    ViewDefinition viewDefinition = getDefaultViewDefinition();

    assertPQLQuery(
        queryRequest,
        "Select span_id FROM SpanEventView WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND is_entry = 123456",
        viewDefinition);
  }

  @Test
  public void testQueryWithOrderBy() {
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    assertPQLQuery(
        buildOrderByQuery(),
        "Select span_id, start_time_millis, end_time_millis FROM SpanEventView WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "order by start_time_millis desc , end_time_millis limit 100",
        viewDefinition);
  }

  @Test
  public void testQueryWithOrderByWithPagination() {
    QueryRequest orderByQueryRequest = buildOrderByQuery();
    Builder builder = QueryRequest.newBuilder(orderByQueryRequest);
    builder.setOffset(1000);
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    assertPQLQuery(
        builder.build(),
        "Select span_id, start_time_millis, end_time_millis FROM SpanEventView WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "order by start_time_millis desc , end_time_millis limit 1000, 100",
        viewDefinition);
  }

  @Test
  public void testQueryWithGroupByWithMultipleAggregates() {
    QueryRequest orderByQueryRequest = buildMultipleGroupByMultipleAggQuery();
    Builder builder = QueryRequest.newBuilder(orderByQueryRequest);
    builder.setLimit(20);
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    assertPQLQuery(
        builder.build(),
        "select service_name, span_name, count(*), avg(duration_millis) from SpanEventView"
            + " where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "and ( start_time_millis > '1570658506605' and end_time_millis < '1570744906673' )"
            + " group by service_name, span_name limit 20",
        viewDefinition);
  }

  @Test
  public void testQueryWithGroupByWithMultipleAggregatesAndOrderBy() {
    QueryRequest orderByQueryRequest = buildMultipleGroupByMultipleAggAndOrderByQuery();
    Builder builder = QueryRequest.newBuilder(orderByQueryRequest);
    builder.setLimit(20);
    ViewDefinition viewDefinition = getDefaultViewDefinition();
    assertPQLQuery(
        builder.build(),
        "select service_name, span_name, count(*), avg(duration_millis) from SpanEventView"
            + " where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "and ( start_time_millis > '1570658506605' and end_time_millis < '1570744906673' )"
            + " group by service_name, span_name order by service_name, avg(duration_millis) desc , count(*) desc  limit 20",
        viewDefinition);
  }

  @Test
  public void testQueryWithDistinctCountAggregation() {
    Filter startTimeFilter =
        createTimeFilter("Span.start_time_millis", Operator.GT, 1570658506605L);
    Filter endTimeFilter = createTimeFilter("Span.end_time_millis", Operator.LT, 1570744906673L);
    QueryRequest queryRequest =
        QueryRequest.newBuilder()
            .addAggregation(
                createFunctionExpression("DISTINCTCOUNT", "Span.id", "distinctcount_span_id"))
            .setFilter(
                Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addChildFilter(startTimeFilter)
                    .addChildFilter(endTimeFilter)
                    .build())
            .setLimit(15)
            .build();

    ViewDefinition viewDefinition = getDefaultViewDefinition();

    assertPQLQuery(
        queryRequest,
        "select distinctcount(span_id) from SpanEventView"
            + " where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "and ( start_time_millis > '1570658506605' and end_time_millis < '1570744906673' )"
            + " limit 15",
        viewDefinition);
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
                createFunctionExpression("DISTINCTCOUNT", "Span.id", "distinctcount_span_id"))
            .setFilter(
                Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addChildFilter(startTimeFilter)
                    .addChildFilter(endTimeFilter)
                    .build())
            .addOrderBy(
                createOrderByExpression(
                    createFunctionExpression("DISTINCTCOUNT", "Span.id", "distinctcount_span_id"),
                    SortOrder.ASC))
            .setLimit(15)
            .build();

    ViewDefinition viewDefinition = getDefaultViewDefinition();

    assertPQLQuery(
        queryRequest,
        "select span_id, distinctcount(span_id) from SpanEventView"
            + " where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "and ( start_time_millis > '1570658506605' and end_time_millis < '1570744906673' )"
            + " group by span_id order by distinctcount(span_id) limit 15",
        viewDefinition);
  }

  @Test
  public void testQueryWithStringArray() {
    Builder builder = QueryRequest.newBuilder();
    ColumnIdentifier spanId = ColumnIdentifier.newBuilder().setColumnName("Span.id").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(spanId).build());

    String spanId1 = "042e5523ff6b2506";
    String spanId2 = "041e5523ff6b2501";
    LiteralConstant spanIds =
        LiteralConstant.newBuilder()
            .setValue(
                Value.newBuilder()
                    .setValueType(ValueType.STRING_ARRAY)
                    .addStringArray(spanId1)
                    .addStringArray(spanId2)
                    .build())
            .build();

    Filter filter =
        Filter.newBuilder()
            .setOperator(Operator.IN)
            .setLhs(Expression.newBuilder().setColumnIdentifier(spanId).build())
            .setRhs(Expression.newBuilder().setLiteral(spanIds).build())
            .build();

    builder.setFilter(filter);

    ViewDefinition viewDefinition = getDefaultViewDefinition();

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
        viewDefinition);
  }

  @Test
  public void testSQLiWithStringArrayFilter() {
    Builder builder = QueryRequest.newBuilder();
    ColumnIdentifier spanId =
        ColumnIdentifier.newBuilder().setColumnName("Span.displaySpanName").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(spanId).build());

    String span1 = "1') OR tenant_id = 'tenant2' and span_name IN ('1";
    LiteralConstant spanIds =
        LiteralConstant.newBuilder()
            .setValue(
                Value.newBuilder()
                    .setValueType(ValueType.STRING_ARRAY)
                    .addStringArray(span1)
                    .build())
            .build();

    Filter filter =
        Filter.newBuilder()
            .setOperator(Operator.IN)
            .setLhs(Expression.newBuilder().setColumnIdentifier(spanId).build())
            .setRhs(Expression.newBuilder().setLiteral(spanIds).build())
            .build();
    builder.setFilter(filter);

    ViewDefinition viewDefinition = getDefaultViewDefinition();

    assertPQLQuery(
        builder.build(),
        "SELECT span_name FROM SpanEventView WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND span_name IN ('1'') OR tenant_id = ''tenant2'' and span_name IN (''1')",
        viewDefinition);
  }

  @Test
  public void testQueryWithLikeOperator() {
    Builder builder = QueryRequest.newBuilder();
    ColumnIdentifier spanId = ColumnIdentifier.newBuilder().setColumnName("Span.id").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(spanId).build());

    Filter likeFilter =
        Filter.newBuilder()
            .setOperator(Operator.LIKE)
            .setLhs(Expression.newBuilder().setColumnIdentifier(spanId).build())
            .setRhs(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(Value.newBuilder().setString("042e5523ff6b2506").build()))
                    .build())
            .build();
    builder.setFilter(likeFilter);

    ViewDefinition viewDefinition = getDefaultViewDefinition();

    assertPQLQuery(
        builder.build(),
        "SELECT span_id FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND REGEXP_LIKE(span_id,'042e5523ff6b2506')",
        viewDefinition);
  }

  @Test
  public void testQueryWithContainsKeyOperator() {
    Builder builder = QueryRequest.newBuilder();
    ColumnIdentifier spanTag = ColumnIdentifier.newBuilder().setColumnName("Span.tags").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(spanTag).build());

    LiteralConstant tag =
        LiteralConstant.newBuilder()
            .setValue(
                Value.newBuilder()
                    .setValueType(ValueType.STRING_ARRAY)
                    .addStringArray("FLAGS")
                    .addStringArray("0")
                    .build())
            .build();

    Filter likeFilter =
        Filter.newBuilder()
            .setOperator(Operator.CONTAINS_KEY)
            .setLhs(Expression.newBuilder().setColumnIdentifier(spanTag).build())
            .setRhs(Expression.newBuilder().setLiteral(tag).build())
            .build();
    builder.setFilter(likeFilter);

    ViewDefinition viewDefinition = getDefaultViewDefinition();

    assertPQLQuery(
        builder.build(),
        "SELECT tags__keys, tags__values FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND tags__keys = 'flags'",
        viewDefinition);
  }

  @Test
  public void testQueryWithContainsKeyValueOperator() {
    Builder builder = QueryRequest.newBuilder();
    ColumnIdentifier spanTag = ColumnIdentifier.newBuilder().setColumnName("Span.tags").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(spanTag).build());

    LiteralConstant tag =
        LiteralConstant.newBuilder()
            .setValue(
                Value.newBuilder()
                    .setValueType(ValueType.STRING_ARRAY)
                    .addStringArray("FLAGS")
                    .addStringArray("0")
                    .build())
            .build();

    Filter likeFilter =
        Filter.newBuilder()
            .setOperator(Operator.CONTAINS_KEYVALUE)
            .setLhs(Expression.newBuilder().setColumnIdentifier(spanTag).build())
            .setRhs(Expression.newBuilder().setLiteral(tag).build())
            .build();
    builder.setFilter(likeFilter);

    ViewDefinition viewDefinition = getDefaultViewDefinition();

    assertPQLQuery(
        builder.build(),
        "SELECT tags__keys, tags__values FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND tags__keys = 'flags' and tags__values = '0' and mapvalue(tags__keys,'flags',tags__values) = '0'",
        viewDefinition);
  }

  @Test
  public void testQueryWithBytesColumnWithValidId() {
    Builder builder = QueryRequest.newBuilder();

    // create selections
    ColumnIdentifier spanId = ColumnIdentifier.newBuilder().setColumnName("Span.id").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(spanId).build());

    // create NEQ filter
    Filter parentIdFilter =
        QueryRequestBuilderUtils.createColumnValueFilter(
                "Span.attributes.parent_span_id", Operator.EQ, "042e5523ff6b2506")
            .build();

    Filter andFilter =
        Filter.newBuilder().setOperator(Operator.AND).addChildFilter(parentIdFilter).build();
    builder.setFilter(andFilter);
    builder.setLimit(5);

    QueryRequest request = builder.build();
    ViewDefinition viewDefinition = getDefaultViewDefinition();

    assertPQLQuery(
        request,
        "SELECT span_id FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND ( parent_span_id = '042e5523ff6b2506' ) limit 5",
        viewDefinition);
  }

  @Test
  public void testQueryWithBytesColumnWithInValidId() {
    Builder builder = QueryRequest.newBuilder();

    // create selections
    ColumnIdentifier spanId = ColumnIdentifier.newBuilder().setColumnName("Span.id").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(spanId).build());

    // create NEQ filter
    Filter parentIdFilter =
        QueryRequestBuilderUtils.createColumnValueFilter(
                "Span.attributes.parent_span_id", Operator.EQ, "042e5523ff6b250L")
            .build();

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

    // create selections
    ColumnIdentifier spanId = ColumnIdentifier.newBuilder().setColumnName("Span.id").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(spanId).build());

    // create NEQ filter
    Filter parentIdFilter =
        QueryRequestBuilderUtils.createColumnValueFilter(
                "Span.attributes.parent_span_id", Operator.NEQ, "null")
            .build();

    Filter andFilter =
        Filter.newBuilder().setOperator(Operator.AND).addChildFilter(parentIdFilter).build();
    builder.setFilter(andFilter);
    builder.setLimit(5);

    QueryRequest request = builder.build();
    ViewDefinition viewDefinition = getDefaultViewDefinition();

    assertPQLQuery(
        request,
        "SELECT span_id FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND ( parent_span_id != '' ) limit 5",
        viewDefinition);
  }

  @Test
  public void testQueryWithBytesColumnWithEmptyId() {
    Builder builder = QueryRequest.newBuilder();

    // create selections
    ColumnIdentifier spanId = ColumnIdentifier.newBuilder().setColumnName("Span.id").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(spanId).build());

    // create NEQ filter
    Filter parentIdFilter =
        QueryRequestBuilderUtils.createColumnValueFilter(
                "Span.attributes.parent_span_id", Operator.NEQ, "''")
            .build();

    Filter andFilter =
        Filter.newBuilder().setOperator(Operator.AND).addChildFilter(parentIdFilter).build();
    builder.setFilter(andFilter);
    builder.setLimit(5);

    QueryRequest request = builder.build();
    ViewDefinition viewDefinition = getDefaultViewDefinition();

    assertPQLQuery(
        request,
        "SELECT span_id FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND ( parent_span_id != '' ) limit 5",
        viewDefinition);
  }

  @Test
  public void testQueryWithBytesColumnInFilter() {
    Builder builder = QueryRequest.newBuilder();
    ColumnIdentifier durationColumn =
        ColumnIdentifier.newBuilder().setColumnName("Span.metrics.duration_millis").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(durationColumn));

    ColumnIdentifier.Builder spanId = ColumnIdentifier.newBuilder().setColumnName("Span.id");
    // Though span id is bytes in Pinot, top layers send the value as hex string.
    Value.Builder value =
        Value.newBuilder()
            .setValueType(ValueType.STRING_ARRAY)
            .addAllStringArray(List.of("042e5523ff6b2506"));
    builder.setFilter(
        Filter.newBuilder()
            .setLhs(Expression.newBuilder().setColumnIdentifier(spanId))
            .setOperator(Operator.IN)
            .setRhs(
                Expression.newBuilder().setLiteral(LiteralConstant.newBuilder().setValue(value))));

    ViewDefinition viewDefinition = getDefaultViewDefinition();

    assertPQLQuery(
        builder.build(),
        "SELECT duration_millis FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND span_id IN ('042e5523ff6b2506')",
        viewDefinition);
  }

  @Test
  public void testQueryWithStringColumnWithNullString() {
    Builder builder = QueryRequest.newBuilder();

    // create selections
    ColumnIdentifier spanId = ColumnIdentifier.newBuilder().setColumnName("Span.id").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(spanId).build());

    // create NEQ filter
    Filter parentIdFilter =
        QueryRequestBuilderUtils.createColumnValueFilter("Span.id", Operator.NEQ, "null").build();

    Filter andFilter =
        Filter.newBuilder().setOperator(Operator.AND).addChildFilter(parentIdFilter).build();
    builder.setFilter(andFilter);
    builder.setLimit(5);

    QueryRequest request = builder.build();
    ViewDefinition viewDefinition = getDefaultViewDefinition();

    assertPQLQuery(
        request,
        "SELECT span_id FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND ( span_id != '' ) limit 5",
        viewDefinition);
  }

  @Test
  public void testQueryWithLongColumn() {
    Builder builder = QueryRequest.newBuilder();

    // create selections
    ColumnIdentifier spanId = ColumnIdentifier.newBuilder().setColumnName("Span.id").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(spanId).build());

    // create an and filter with Long literal type
    ColumnIdentifier durationColumn =
        ColumnIdentifier.newBuilder().setColumnName("Span.metrics.duration_millis").build();
    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.GE)
            .setLhs(Expression.newBuilder().setColumnIdentifier(durationColumn).build())
            .setRhs(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder()
                                    .setValueType(ValueType.LONG)
                                    .setLong(1000L)
                                    .build()))
                    .build())
            .build();

    builder.setFilter(andFilter);
    builder.setLimit(5);

    QueryRequest request = builder.build();
    ViewDefinition viewDefinition = getDefaultViewDefinition();

    assertPQLQuery(
        request,
        "SELECT span_id FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND duration_millis >= 1000 limit 5",
        viewDefinition);
  }

  @Test
  public void testQueryWithLongColumnWithLikeFilter() {
    Builder builder = QueryRequest.newBuilder();
    ColumnIdentifier spanId = ColumnIdentifier.newBuilder().setColumnName("Span.id").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(spanId).build());

    ColumnIdentifier durationColumn =
        ColumnIdentifier.newBuilder().setColumnName("Span.metrics.duration_millis").build();
    Filter likeFilter =
        Filter.newBuilder()
            .setOperator(Operator.LIKE)
            .setLhs(Expression.newBuilder().setColumnIdentifier(durationColumn).build())
            .setRhs(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder()
                                    .setValueType(ValueType.LONG)
                                    .setLong(5000L)
                                    .build()))
                    .build())
            .build();
    builder.setFilter(likeFilter);

    ViewDefinition viewDefinition = getDefaultViewDefinition();

    assertPQLQuery(
        builder.build(),
        "SELECT span_id FROM SpanEventView "
            + "WHERE "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "AND REGEXP_LIKE(duration_millis,5000)",
        viewDefinition);
  }

  @Test
  public void testQueryWithPercentileAggregation() {
    Filter startTimeFilter =
        createTimeFilter("Span.start_time_millis", Operator.GT, 1570658506605L);
    Filter endTimeFilter = createTimeFilter("Span.end_time_millis", Operator.LT, 1570744906673L);
    Expression percentileAgg =
        Expression.newBuilder()
            .setFunction(
                Function.newBuilder()
                    .setAlias("P99_duration")
                    .setFunctionName("PERCENTILE99")
                    .addArguments(
                        Expression.newBuilder()
                            .setColumnIdentifier(
                                ColumnIdentifier.newBuilder()
                                    .setColumnName("Span.metrics.duration_millis"))))
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

    assertPQLQuery(
        queryRequest,
        "select PERCENTILETDIGEST99(duration_millis) from SpanEventView"
            + " where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' "
            + "and ( start_time_millis > '1570658506605' and end_time_millis < '1570744906673' )"
            + " limit 15",
        viewDefinition);
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
                    .addArguments(createNullStringLiteralExpression()))
            .build();

    Expression conditionalNumber =
        Expression.newBuilder()
            .setFunction(
                Function.newBuilder()
                    .setFunctionName(QueryFunctionConstants.QUERY_FUNCTION_CONDITIONAL)
                    .addArguments(createStringLiteralValueExpression("true"))
                    .addArguments(createColumnExpression("Span.metrics.duration_millis"))
                    .addArguments(createNullNumberLiteralExpression()))
            .build();

    QueryRequest queryRequest =
        QueryRequest.newBuilder()
            .addSelection(conditionalString)
            .addSelection(conditionalNumber)
            .setLimit(15)
            .build();

    ViewDefinition viewDefinition = getDefaultViewDefinition();

    assertPQLQuery(
        queryRequest,
        "select conditional('true',span_id,'null'), conditional('true',duration_millis,0)"
            + " from SpanEventView"
            + " where "
            + viewDefinition.getTenantIdColumn()
            + " = '"
            + TENANT_ID
            + "' limit 15",
        viewDefinition);
  }

  @Test
  public void testQueryWithAverageRateInOrderBy() {

    ViewDefinition viewDefinition = getServiceViewDefinition();

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
        viewDefinition);
  }

  private Filter createTimeFilter(String columnName, Operator op, long value) {
    ColumnIdentifier startTimeColumn =
        ColumnIdentifier.newBuilder().setColumnName(columnName).build();
    Expression lhs = Expression.newBuilder().setColumnIdentifier(startTimeColumn).build();

    LiteralConstant constant =
        LiteralConstant.newBuilder()
            .setValue(Value.newBuilder().setString(String.valueOf(value)).build())
            .build();
    Expression rhs = Expression.newBuilder().setLiteral(constant).build();
    return Filter.newBuilder().setLhs(lhs).setOperator(op).setRhs(rhs).build();
  }

  private Filter createLongTimeFilter(String columnName, Operator op, long value) {
    ColumnIdentifier startTimeColumn =
        ColumnIdentifier.newBuilder().setColumnName(columnName).build();
    Expression lhs = Expression.newBuilder().setColumnIdentifier(startTimeColumn).build();

    LiteralConstant constant =
        LiteralConstant.newBuilder()
            .setValue(Value.newBuilder().setValueType(ValueType.LONG).setLong(value).build())
            .build();
    Expression rhs = Expression.newBuilder().setLiteral(constant).build();
    return Filter.newBuilder().setLhs(lhs).setOperator(op).setRhs(rhs).build();
  }

  private Filter createStringFilter(String columnName, Operator op, String value) {
    ColumnIdentifier booleanColumn =
        ColumnIdentifier.newBuilder().setColumnName(columnName).build();
    Expression lhs = Expression.newBuilder().setColumnIdentifier(booleanColumn).build();

    LiteralConstant constant =
        LiteralConstant.newBuilder()
            .setValue(Value.newBuilder().setValueType(ValueType.STRING).setString(value).build())
            .build();
    Expression rhs = Expression.newBuilder().setLiteral(constant).build();
    return Filter.newBuilder().setLhs(lhs).setOperator(op).setRhs(rhs).build();
  }

  private Filter createBooleanFilter(String columnName, Operator op, boolean value) {
    ColumnIdentifier booleanColumn =
        ColumnIdentifier.newBuilder().setColumnName(columnName).build();
    Expression lhs = Expression.newBuilder().setColumnIdentifier(booleanColumn).build();

    LiteralConstant constant =
        LiteralConstant.newBuilder()
            .setValue(Value.newBuilder().setValueType(ValueType.BOOL).setBoolean(value).build())
            .build();
    Expression rhs = Expression.newBuilder().setLiteral(constant).build();
    return Filter.newBuilder().setLhs(lhs).setOperator(op).setRhs(rhs).build();
  }

  private Filter createTimestampFilter(String columnName, Operator op, long value) {
    ColumnIdentifier booleanColumn =
        ColumnIdentifier.newBuilder().setColumnName(columnName).build();
    Expression lhs = Expression.newBuilder().setColumnIdentifier(booleanColumn).build();

    LiteralConstant constant =
        LiteralConstant.newBuilder()
            .setValue(
                Value.newBuilder().setValueType(ValueType.TIMESTAMP).setTimestamp(value).build())
            .build();
    Expression rhs = Expression.newBuilder().setLiteral(constant).build();
    return Filter.newBuilder().setLhs(lhs).setOperator(op).setRhs(rhs).build();
  }

  private Filter createDoubleFilter(String columnName, Operator op, double value) {
    ColumnIdentifier booleanColumn =
        ColumnIdentifier.newBuilder().setColumnName(columnName).build();
    Expression lhs = Expression.newBuilder().setColumnIdentifier(booleanColumn).build();

    LiteralConstant constant =
        LiteralConstant.newBuilder()
            .setValue(Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(value).build())
            .build();
    Expression rhs = Expression.newBuilder().setLiteral(constant).build();
    return Filter.newBuilder().setLhs(lhs).setOperator(op).setRhs(rhs).build();
  }

  private Filter createFloatFilter(String columnName, Operator op, float value) {
    ColumnIdentifier booleanColumn =
        ColumnIdentifier.newBuilder().setColumnName(columnName).build();
    Expression lhs = Expression.newBuilder().setColumnIdentifier(booleanColumn).build();

    LiteralConstant constant =
        LiteralConstant.newBuilder()
            .setValue(Value.newBuilder().setValueType(ValueType.FLOAT).setFloat(value).build())
            .build();
    Expression rhs = Expression.newBuilder().setLiteral(constant).build();
    return Filter.newBuilder().setLhs(lhs).setOperator(op).setRhs(rhs).build();
  }

  private Filter createIntFilter(String columnName, Operator op, int value) {
    ColumnIdentifier booleanColumn =
        ColumnIdentifier.newBuilder().setColumnName(columnName).build();
    Expression lhs = Expression.newBuilder().setColumnIdentifier(booleanColumn).build();

    LiteralConstant constant =
        LiteralConstant.newBuilder()
            .setValue(Value.newBuilder().setValueType(ValueType.INT).setInt(value).build())
            .build();
    Expression rhs = Expression.newBuilder().setLiteral(constant).build();
    return Filter.newBuilder().setLhs(lhs).setOperator(op).setRhs(rhs).build();
  }

  private Filter createNullStringFilter(String columnName, Operator op) {
    ColumnIdentifier booleanColumn =
        ColumnIdentifier.newBuilder().setColumnName(columnName).build();
    Expression lhs = Expression.newBuilder().setColumnIdentifier(booleanColumn).build();

    LiteralConstant constant =
        LiteralConstant.newBuilder()
            .setValue(Value.newBuilder().setValueType(ValueType.NULL_STRING).build())
            .build();
    Expression rhs = Expression.newBuilder().setLiteral(constant).build();
    return Filter.newBuilder().setLhs(lhs).setOperator(op).setRhs(rhs).build();
  }

  private QueryRequest buildAvgRateQueryForOrderBy() {
    Builder builder = QueryRequest.newBuilder();

    ColumnIdentifier serviceId = ColumnIdentifier.newBuilder().setColumnName("SERVICE.id").build();
    ColumnIdentifier serviceName =
        ColumnIdentifier.newBuilder().setColumnName("SERVICE.name").build();
    ColumnIdentifier serviceErrorCount =
        ColumnIdentifier.newBuilder().setColumnName("SERVICE.errorCount").build();

    Function.Builder countFunction =
        Function.newBuilder()
            .setFunctionName("COUNT")
            .addArguments(Expression.newBuilder().setColumnIdentifier(serviceId).build());

    Function.Builder avgrateFunction =
        Function.newBuilder()
            .setFunctionName("AVGRATE")
            .addArguments(Expression.newBuilder().setColumnIdentifier(serviceErrorCount).build());

    Filter nullCheckFilter = createNullStringFilter("SERVICE.id", Operator.NEQ);
    Filter startTimeFilter = createLongTimeFilter("SERVICE.startTime", Operator.GE, 1637297304041L);
    Filter endTimeFilter = createLongTimeFilter("SERVICE.startTime", Operator.LT, 1637300904041L);
    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .addChildFilter(nullCheckFilter)
            .build();
    builder.setFilter(andFilter);

    builder.addSelection(Expression.newBuilder().setColumnIdentifier(serviceId).build());
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(serviceName).build());
    builder.addSelection(Expression.newBuilder().setFunction(countFunction).build());

    builder.addGroupBy(Expression.newBuilder().setColumnIdentifier(serviceId).build());
    builder.addGroupBy(Expression.newBuilder().setColumnIdentifier(serviceName).build());

    builder.addOrderBy(
        createOrderByExpression(
            Expression.newBuilder().setFunction(avgrateFunction), SortOrder.ASC));

    builder.setLimit(10000);
    return builder.build();
  }

  private QueryRequest buildOrderByQuery() {
    Builder builder = QueryRequest.newBuilder();
    ColumnIdentifier spanId = ColumnIdentifier.newBuilder().setColumnName("Span.id").build();
    ColumnIdentifier startTimeColumn =
        ColumnIdentifier.newBuilder().setColumnName("Span.start_time_millis").build();
    ColumnIdentifier endTimeColumn =
        ColumnIdentifier.newBuilder().setColumnName("Span.end_time_millis").build();

    builder.addSelection(Expression.newBuilder().setColumnIdentifier(spanId).build());
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(startTimeColumn).build());
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(endTimeColumn).build());

    builder.addOrderBy(
        createOrderByExpression(
            Expression.newBuilder().setColumnIdentifier(startTimeColumn), SortOrder.DESC));
    builder.addOrderBy(
        createOrderByExpression(
            Expression.newBuilder().setColumnIdentifier(endTimeColumn), SortOrder.ASC));

    builder.setLimit(100);
    return builder.build();
  }

  private QueryRequest buildMultipleGroupByMultipleAggQuery() {
    Builder builder = QueryRequest.newBuilder();
    builder.addAggregation(QueryRequestBuilderUtils.createCountByColumnSelection("Span.id"));
    Function.Builder avg =
        Function.newBuilder()
            .setFunctionName("AVG")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("Span.duration_millis")));
    builder.addAggregation(Expression.newBuilder().setFunction(avg));

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

    builder.addGroupBy(
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder().setColumnName("Span.serviceName").build()));
    builder.addGroupBy(
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder().setColumnName("Span.displaySpanName").build()));
    return builder.build();
  }

  private QueryRequest buildMultipleGroupByMultipleAggAndOrderByQuery() {
    Builder builder = QueryRequest.newBuilder();
    builder.addAggregation(QueryRequestBuilderUtils.createCountByColumnSelection("Span.id"));
    Function.Builder avg =
        Function.newBuilder()
            .setFunctionName("AVG")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("Span.duration_millis")));
    builder.addAggregation(Expression.newBuilder().setFunction(avg));

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

    builder.addGroupBy(
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder().setColumnName("Span.serviceName").build()));
    builder.addGroupBy(
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder().setColumnName("Span.displaySpanName").build()));

    builder.addOrderBy(
        createOrderByExpression(createColumnExpression("Span.serviceName"), SortOrder.ASC));
    builder.addOrderBy(
        createOrderByExpression(
            createFunctionExpression("AVG", "Span.duration_millis", "avg_duration_millis"),
            SortOrder.DESC));
    builder.addOrderBy(
        createOrderByExpression(
            createFunctionExpression("COUNT", "Span.id", "count_span_id"), SortOrder.DESC));
    return builder.build();
  }

  private QueryRequest buildSimpleQueryWithFilter(Filter filter) {
    Builder builder = QueryRequest.newBuilder();
    ColumnIdentifier columnName = ColumnIdentifier.newBuilder().setColumnName("Span.id").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(columnName).build());

    builder.setFilter(filter);

    return builder.build();
  }

  private void assertPQLQuery(
      QueryRequest queryRequest, String expectedQuery, ViewDefinition viewDefinition) {
    QueryRequestToPinotSQLConverter converter =
        new QueryRequestToPinotSQLConverter(viewDefinition, new PinotFunctionConverter());
    ExecutionContext executionContext = new ExecutionContext("__default", queryRequest);
    executionContext.setTimeFilterColumn("SERVICE.startTime");
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
}
