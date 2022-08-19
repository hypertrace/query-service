package org.hypertrace.core.query.service.postgres;

import static org.hypertrace.core.query.service.QueryRequestUtil.getLogicalColumnName;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import io.micrometer.core.instrument.Timer;
import io.reactivex.rxjava3.core.Observable;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.SneakyThrows;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.QueryCost;
import org.hypertrace.core.query.service.RequestHandler;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Expression.ValueCase;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.OrderByExpression;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.Row.Builder;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;
import org.hypertrace.core.query.service.postgres.PostgresClientFactory.PostgresClient;
import org.hypertrace.core.query.service.postgres.converters.PostgresFunctionConverter;
import org.hypertrace.core.query.service.postgres.converters.PostgresFunctionConverterConfig;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** RequestHandler to handle queries by fetching data from Postgres. */
public class PostgresBasedRequestHandler implements RequestHandler {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresBasedRequestHandler.class);

  public static final String TABLE_DEFINITION_CONFIG_KEY = "tableDefinition";
  private static final String TENANT_COLUMN_NAME_CONFIG_KEY = "tenantColumnName";
  private static final String COUNT_COLUMN_NAME_CONFIG_KEY = "countColumnName";
  private static final String START_TIME_ATTRIBUTE_NAME_CONFIG_KEY = "startTimeAttributeName";
  private static final String SLOW_QUERY_THRESHOLD_MS_CONFIG = "slowQueryThresholdMs";

  private static final int DEFAULT_SLOW_QUERY_THRESHOLD_MS = 3000;
  private static final Set<Operator> GTE_OPERATORS = Set.of(Operator.GE, Operator.GT, Operator.EQ);

  // string values equivalent for null value of different data types
  // this is required to keep null values equivalent to default values for
  // various data types in pinot implementation
  private static final Value NULL_STRING_EQ_STRING_VALUE =
      Value.newBuilder().setValueType(ValueType.STRING).setString("null").build();
  private static final Value NULL_INTEGER_EQ_STRING_VALUE =
      Value.newBuilder().setValueType(ValueType.STRING).setString("0").build();
  private static final Value NULL_FLOAT_EQ_STRING_VALUE =
      Value.newBuilder().setValueType(ValueType.STRING).setString("0.0").build();
  private static final Value NULL_BOOLEAN_EQ_STRING_VALUE =
      Value.newBuilder().setValueType(ValueType.STRING).setString("false").build();

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String name;
  private TableDefinition tableDefinition;
  private Optional<String> startTimeAttributeName;
  private QueryRequestToPostgresSQLConverter request2PostgresSqlConverter;
  private final PostgresClientFactory postgresClientFactory;

  private final JsonFormat.Printer protoJsonPrinter =
      JsonFormat.printer().omittingInsignificantWhitespace();

  private Timer postgresQueryExecutionTimer;
  private int slowQueryThreshold = DEFAULT_SLOW_QUERY_THRESHOLD_MS;

  PostgresBasedRequestHandler(String name, Config config) {
    this(name, config, PostgresClientFactory.get());
  }

  PostgresBasedRequestHandler(
      String name, Config config, PostgresClientFactory postgresClientFactory) {
    this.name = name;
    this.postgresClientFactory = postgresClientFactory;
    this.processConfig(config);
  }

  private void initMetrics() {
    // Registry the latency metric with handler as a tag.
    this.postgresQueryExecutionTimer =
        PlatformMetricsRegistry.registerTimer(
            "postgres.query.latency", Map.of("handler", name), true);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Optional<String> getTimeFilterColumn() {
    return this.startTimeAttributeName;
  }

  private void processConfig(Config config) {

    if (!config.hasPath(TENANT_COLUMN_NAME_CONFIG_KEY)) {
      throw new RuntimeException(
          TENANT_COLUMN_NAME_CONFIG_KEY + " is not defined in the " + name + " request handler.");
    }

    String tenantColumnName = config.getString(TENANT_COLUMN_NAME_CONFIG_KEY);
    Optional<String> countColumnName =
        config.hasPath(COUNT_COLUMN_NAME_CONFIG_KEY)
            ? Optional.of(config.getString(COUNT_COLUMN_NAME_CONFIG_KEY))
            : Optional.empty();

    this.tableDefinition =
        TableDefinition.parse(
            config.getConfig(TABLE_DEFINITION_CONFIG_KEY), tenantColumnName, countColumnName);

    this.startTimeAttributeName =
        config.hasPath(START_TIME_ATTRIBUTE_NAME_CONFIG_KEY)
            ? Optional.of(config.getString(START_TIME_ATTRIBUTE_NAME_CONFIG_KEY))
            : Optional.empty();

    this.request2PostgresSqlConverter =
        new QueryRequestToPostgresSQLConverter(
            tableDefinition,
            new PostgresFunctionConverter(
                tableDefinition, new PostgresFunctionConverterConfig(config)));

    if (config.hasPath(SLOW_QUERY_THRESHOLD_MS_CONFIG)) {
      this.slowQueryThreshold = config.getInt(SLOW_QUERY_THRESHOLD_MS_CONFIG);
    }
    LOG.info(
        "Using {}ms as the threshold for logging slow queries of handler: {}",
        slowQueryThreshold,
        name);

    initMetrics();
  }

  /**
   * Returns a QueryCost that is an indication of whether the given query can be handled by this
   * handler and if so, how costly is it to handle that query.
   *
   * <p>A query can usually be handled by Postgres handler if the Postgres view of this handler has
   * all the columns that are referenced in the incoming query. If the Postgres view is a filtered
   * view on some view column filters, the incoming query has to have those filters to match the
   * view.
   */
  @Override
  public QueryCost canHandle(QueryRequest request, ExecutionContext executionContext) {
    Set<String> referencedColumns = executionContext.getReferencedColumns();

    Preconditions.checkArgument(!referencedColumns.isEmpty());
    for (String referencedColumn : referencedColumns) {
      if (!tableDefinition.containsColumn(referencedColumn)) {
        return QueryCost.UNSUPPORTED;
      }
    }

    if (!this.viewDefinitionSupportsFilter(tableDefinition, request.getFilter())) {
      return QueryCost.UNSUPPORTED;
    }

    double cost;

    long requestStartTime = getRequestStartTime(request.getFilter());
    // check if this view contains data from the requested start time
    if (requestStartTime < System.currentTimeMillis() - tableDefinition.getRetentionTimeMillis()) {
      // prefer to get data from the view which has max retention time. Ensure 0.5 <= cost <= 1
      cost = 1 - tableDefinition.getRetentionTimeMillis() / (Long.MAX_VALUE * 2D);
    } else {
      // prefer to get data from the view which has the finest granularity. Ensure cost <= 0.5
      cost = tableDefinition.getTimeGranularityMillis() / (Long.MAX_VALUE * 2D);
    }

    return new QueryCost(cost);
  }

  private boolean viewDefinitionSupportsFilter(TableDefinition tableDefinition, Filter filter) {
    // If the view has any column filters, check if the query has those filters as **mandatory**
    // filters. If not, the view can't serve the query.
    Map<String, TableColumnFilter> viewFilterMap = tableDefinition.getColumnFilterMap();
    return viewFilterMap.isEmpty()
        || viewFilterMap.keySet().equals(this.getMatchingViewFilterColumns(filter, viewFilterMap));
  }

  private long getRequestStartTime(Filter filter) {
    long requestStartTime = Long.MAX_VALUE;

    if (lhsIsStartTimeAttribute(filter.getLhs())
        && GTE_OPERATORS.contains(filter.getOperator())
        && rhsHasLongValue(filter.getRhs())) {
      long filterStartTime = filter.getRhs().getLiteral().getValue().getLong();
      requestStartTime = Math.min(requestStartTime, filterStartTime);
    }

    for (Filter childFilter : filter.getChildFilterList()) {
      requestStartTime = Math.min(requestStartTime, getRequestStartTime(childFilter));
    }

    return requestStartTime;
  }

  private boolean lhsIsStartTimeAttribute(Expression lhs) {
    return startTimeAttributeName.isPresent()
        && startTimeAttributeName.equals(getLogicalColumnName(lhs));
  }

  private boolean rhsHasLongValue(Expression rhs) {
    return rhs.hasLiteral() && rhs.getLiteral().getValue().getValueType() == ValueType.LONG;
  }

  /**
   * Method to return the set of columns from the given filter which match the columns in the given
   * viewFilterMap.
   */
  private Set<String> getMatchingViewFilterColumns(
      Filter filter, Map<String, TableColumnFilter> viewFilterMap) {
    // 1. Basic case: Filter is a leaf node. Check if the column exists in view filters and
    // return it.
    if (filter.getChildFilterCount() == 0) {
      return doesSingleViewFilterMatchLeafQueryFilter(viewFilterMap, filter)
          ? Set.of(getLogicalColumnName(filter.getLhs()).orElseThrow(IllegalArgumentException::new))
          : Set.of();
    } else {
      // 2. Internal filter node. Recursively get the matching nodes from children.
      List<Set<String>> results =
          filter.getChildFilterList().stream()
              .map(f -> getMatchingViewFilterColumns(f, viewFilterMap))
              .collect(Collectors.toList());

      Set<String> result = results.get(0);
      for (Set<String> set : results.subList(1, results.size())) {
        // If the operation is OR, we need to get intersection of columns from all the children.
        // Otherwise, the operation should be AND and we can get union of all columns.
        result =
            filter.getOperator() == Operator.OR
                ? Sets.intersection(result, set)
                : Sets.union(result, set);
      }
      return result;
    }
  }

  /**
   * Method to check if the given ViewColumnFilter matches the given query filter. A match here
   * means the view column is superset of what the query filter is looking for, need not be an exact
   * match.
   */
  private boolean doesSingleViewFilterMatchLeafQueryFilter(
      Map<String, TableColumnFilter> viewFilterMap, Filter queryFilter) {

    if (queryFilter.getOperator() != Operator.IN && queryFilter.getOperator() != Operator.EQ) {
      return false;
    }

    TableColumnFilter tableColumnFilter =
        getLogicalColumnName(queryFilter.getLhs()).map(viewFilterMap::get).orElse(null);
    if (tableColumnFilter == null) {
      return false;
    }

    switch (tableColumnFilter.getOperator()) {
      case IN:
        return isSubSet(tableColumnFilter.getValues(), queryFilter.getRhs());
      case EQ:
        return isEquals(tableColumnFilter.getValues(), queryFilter.getRhs());
      default:
        throw new IllegalArgumentException(
            "Unsupported view filter operator: " + tableColumnFilter.getOperator());
    }
  }

  /** Checks that the values from the given expression are a subset of the given set. */
  private boolean isSubSet(Set<String> values, Expression expression) {
    if (!expression.hasLiteral()) {
      return false;
    }

    return values.containsAll(getExpressionValues(expression.getLiteral()));
  }

  /** Checks that the values from the given expression are a subset of the given set. */
  private boolean isEquals(Set<String> values, Expression expression) {
    if (!expression.hasLiteral()) {
      return false;
    }

    return values.equals(getExpressionValues(expression.getLiteral()));
  }

  private Set<String> getExpressionValues(LiteralConstant literalConstant) {
    Set<String> expressionValues = new HashSet<>();
    Value value = literalConstant.getValue();
    switch (value.getValueType()) {
      case STRING:
        expressionValues.add(value.getString());
        break;
      case STRING_ARRAY:
        expressionValues.addAll(value.getStringArrayList());
        break;
      case INT:
        expressionValues.add(String.valueOf(value.getInt()));
        break;
      case INT_ARRAY:
        expressionValues.addAll(
            value.getIntArrayList().stream().map(Object::toString).collect(Collectors.toSet()));
        break;
      case LONG:
        expressionValues.add(String.valueOf(value.getLong()));
        break;
      case LONG_ARRAY:
        expressionValues.addAll(
            value.getLongArrayList().stream().map(Object::toString).collect(Collectors.toSet()));
        break;
      case DOUBLE:
        expressionValues.add(String.valueOf(value.getDouble()));
        break;
      case DOUBLE_ARRAY:
        expressionValues.addAll(
            value.getDoubleArrayList().stream().map(Object::toString).collect(Collectors.toSet()));
        break;
      case FLOAT:
        expressionValues.add(String.valueOf(value.getFloat()));
        break;
      case FLOAT_ARRAY:
        expressionValues.addAll(
            value.getFloatArrayList().stream().map(Object::toString).collect(Collectors.toSet()));
        break;
      case BOOL:
        expressionValues.add(String.valueOf(value.getBoolean()).toLowerCase());
        break;
      case BOOLEAN_ARRAY:
        expressionValues.addAll(
            value.getBooleanArrayList().stream()
                .map(b -> b.toString().toLowerCase())
                .collect(Collectors.toSet()));
        break;
      default:
        // Ignore the rest of the types for now.
        throw new IllegalArgumentException("Unsupported value type in subset check.");
    }
    return expressionValues;
  }

  @Override
  public Observable<Row> handleRequest(
      QueryRequest originalRequest, ExecutionContext executionContext) {
    try {
      Stopwatch stopwatch = Stopwatch.createStarted();
      validateQueryRequest(executionContext, originalRequest);

      QueryRequest request;
      // Rewrite the request filter after applying the view filters.
      if (!tableDefinition.getColumnFilterMap().isEmpty()
          && !Filter.getDefaultInstance().equals(originalRequest.getFilter())) {
        request =
            rewriteRequestWithViewFiltersApplied(
                originalRequest, tableDefinition.getColumnFilterMap());
      } else {
        request = originalRequest;
      }

      Entry<String, Params> sql =
          request2PostgresSqlConverter.toSQL(
              executionContext, request, executionContext.getAllSelections());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Trying to execute SQL: [ {} ] by RequestHandler: [ {} ]", sql, this.getName());
      }

      Observable<Row> rowObservable =
          postgresQueryExecutionTimer.recordCallable(
              () -> executeQuery(sql.getKey(), sql.getValue()));

      // need to merge data especially for Postgres. That's why we need to track the map columns
      return rowObservable.doOnComplete(
          () -> {
            long requestTimeMs = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
            if (requestTimeMs > slowQueryThreshold) {
              try {
                LOG.warn(
                    "Query Execution time: {} ms, sqlQuery: {}, queryRequest: {}, executionStats: {}",
                    requestTimeMs,
                    sql.getKey(),
                    protoJsonPrinter.print(request),
                    "Stats not available");
              } catch (InvalidProtocolBufferException ignore) {
                // ignore this exception
              }
            }
          });
    } catch (Throwable error) {
      return Observable.error(error);
    }
  }

  @Nonnull
  private QueryRequest rewriteRequestWithViewFiltersApplied(
      QueryRequest request, Map<String, TableColumnFilter> columnFilterMap) {

    Filter newFilter = removeViewColumnFilter(request.getFilter(), columnFilterMap);
    QueryRequest.Builder builder = QueryRequest.newBuilder(request).clearFilter();
    if (!Filter.getDefaultInstance().equals(newFilter)) {
      builder.setFilter(newFilter);
    }
    return builder.build();
  }

  private Filter removeViewColumnFilter(
      Filter filter, Map<String, TableColumnFilter> columnFilterMap) {
    if (filter.getChildFilterCount() > 0) {
      // Recursively try to remove the filter and eliminate the null nodes.
      Set<Filter> newFilters =
          filter.getChildFilterList().stream()
              .map(f -> removeViewColumnFilter(f, columnFilterMap))
              .filter(f -> !Filter.getDefaultInstance().equals(f))
              .collect(Collectors.toCollection(LinkedHashSet::new));

      if (newFilters.isEmpty()) {
        return Filter.getDefaultInstance();
      } else if (newFilters.size() == 1) {
        return Iterables.getOnlyElement(newFilters);
      } else {
        return Filter.newBuilder(filter).clearChildFilter().addAllChildFilter(newFilters).build();
      }
    } else {
      return rewriteLeafFilter(filter, columnFilterMap);
    }
  }

  private Filter rewriteLeafFilter(
      Filter queryFilter, Map<String, TableColumnFilter> columnFilterMap) {
    TableColumnFilter tableColumnFilter =
        columnFilterMap.get(
            getLogicalColumnName(queryFilter.getLhs()).orElseThrow(IllegalArgumentException::new));
    // If the RHS of both the view filter and query filter match, return empty filter.
    if (tableColumnFilter != null
        && isEquals(tableColumnFilter.getValues(), queryFilter.getRhs())) {
      return Filter.getDefaultInstance();
    }

    // In every other case, retain the query filter.
    return queryFilter;
  }

  public Observable<Row> executeQuery(String statement, Params params) throws SQLException {
    final PostgresClient postgresClient = postgresClientFactory.getPostgresClient(this.getName());
    String resolvedStatement = request2PostgresSqlConverter.resolveStatement(statement, params);
    try (Connection connection = postgresClient.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(resolvedStatement);
        ResultSet resultSet = preparedStatement.executeQuery()) {
      LOG.debug("Query results: [ {} ]", resultSet);
      return convert(resultSet);
    } catch (Exception ex) {
      // Catch this exception to log the Postgres SQL query that caused the issue
      LOG.error("An error occurred while executing: {}", resolvedStatement, ex);
      // Rethrow for the caller to return an error.
      throw new RuntimeException(ex);
    }
  }

  @SneakyThrows
  Observable<Row> convert(ResultSet resultSet) {
    List<Row> rowList = new ArrayList<>();
    while (resultSet.next()) {
      Builder builder = Row.newBuilder();
      ResultSetMetaData metaData = resultSet.getMetaData();
      int columnCount = metaData.getColumnCount();
      if (columnCount > 0) {
        for (int c = 1; c <= columnCount; c++) {
          int colType = metaData.getColumnType(c);
          Value convertedColVal;
          if (colType == Types.ARRAY) {
            Array colVal = resultSet.getArray(c);
            convertedColVal =
                Value.newBuilder()
                    .setValueType(ValueType.STRING)
                    .setString(
                        MAPPER.writeValueAsString(
                            colVal != null ? colVal.getArray() : Collections.emptyList()))
                    .build();
          } else {
            String colVal = resultSet.getString(c);
            convertedColVal =
                colVal != null
                    ? Value.newBuilder().setValueType(ValueType.STRING).setString(colVal).build()
                    : getNullValueEquivalent(metaData.getColumnType(c));
          }
          builder.addColumn(convertedColVal);
        }
      }
      rowList.add(builder.build());
    }
    return Observable.fromIterable(rowList).doOnNext(row -> LOG.debug("collect a row: {}", row));
  }

  private Value getNullValueEquivalent(int columnType) {
    switch (columnType) {
      case Types.BIGINT:
      case Types.INTEGER:
        return NULL_INTEGER_EQ_STRING_VALUE;
      case Types.FLOAT:
      case Types.DOUBLE:
        return NULL_FLOAT_EQ_STRING_VALUE;
      case Types.BOOLEAN:
        return NULL_BOOLEAN_EQ_STRING_VALUE;
      default:
        return NULL_STRING_EQ_STRING_VALUE;
    }
  }

  private void validateQueryRequest(ExecutionContext executionContext, QueryRequest request) {
    // Validate QueryContext and tenant id presence
    Preconditions.checkNotNull(executionContext);
    Preconditions.checkNotNull(executionContext.getTenantId());

    // Validate DISTINCT selections
    if (request.getDistinctSelections()) {
      boolean noGroupBy = request.getGroupByCount() == 0;
      boolean noAggregations = request.getAggregationCount() == 0;
      Preconditions.checkArgument(
          noGroupBy && noAggregations,
          "If distinct selections are requested, there should be no groupBys or aggregations.");
    }

    // Validate attribute expressions
    validateAttributeExpressionFilter(request.getFilter());

    for (Expression expression : executionContext.getAllSelections()) {
      if (isInvalidExpression(expression)) {
        throw new IllegalArgumentException("Invalid Query");
      }
    }

    for (Expression expression : request.getGroupByList()) {
      if (isInvalidExpression(expression)) {
        throw new IllegalArgumentException("Invalid Query");
      }
    }

    for (OrderByExpression orderByExpression : request.getOrderByList()) {
      if (isInvalidExpression(orderByExpression.getExpression())) {
        throw new IllegalArgumentException("Invalid Query");
      }
    }
  }

  private void validateAttributeExpressionFilter(Filter filter) {
    if (filter.getChildFilterCount() > 0) {
      for (Filter childFilter : filter.getChildFilterList()) {
        validateAttributeExpressionFilter(childFilter);
      }
    } else {
      if (isInvalidExpression(filter.getLhs())) {
        throw new IllegalArgumentException("Invalid Query");
      }
    }
  }

  private boolean isInvalidExpression(Expression expression) {
    return expression.getValueCase() == ValueCase.ATTRIBUTE_EXPRESSION
        && expression.getAttributeExpression().hasSubpath()
        && tableDefinition.getColumnType(expression.getAttributeExpression().getAttributeId())
            != ValueType.STRING_MAP;
  }
}
