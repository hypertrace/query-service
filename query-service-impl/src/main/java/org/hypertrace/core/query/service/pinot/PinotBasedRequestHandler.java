package org.hypertrace.core.query.service.pinot;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.client.ResultSetGroup;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.QueryCost;
import org.hypertrace.core.query.service.QueryResultCollector;
import org.hypertrace.core.query.service.RequestHandler;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Expression.ValueCase;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.Row.Builder;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.pinot.PinotClientFactory.PinotClient;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RequestHandler to handle queries by fetching data from Pinot.
 */
public class PinotBasedRequestHandler implements RequestHandler<QueryRequest, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(PinotBasedRequestHandler.class);

  public static final String VIEW_DEFINITION_CONFIG_KEY = "viewDefinition";
  private static final String TENANT_COLUMN_NAME_CONFIG_KEY = "tenantColumnName";
  private static final String SLOW_QUERY_THRESHOLD_MS_CONFIG = "slowQueryThresholdMs";
  private static final String PERCENTILE_AGGREGATION_FUNCTION_CONFIG = "percentileAggFunction";

  private static final int DEFAULT_SLOW_QUERY_THRESHOLD_MS = 3000;

  /**
   * Computing PERCENTILE in Pinot is resource intensive. T-Digest calculation is much faster and
   * reasonably accurate, hence use that as the default.
   */
  private static final String DEFAULT_PERCENTILE_AGGREGATION_FUNCTION = "PERCENTILETDIGEST";

  private String name;
  private ViewDefinition viewDefinition;
  private QueryRequestToPinotSQLConverter request2PinotSqlConverter;
  private final PinotMapConverter pinotMapConverter;
  // The implementations of ResultSet are package private and hence there's no way to determine the
  // shape of the results
  // other than to do string comparison on the simple class names. In order to be able to unit test
  // the logic for
  // parsing the Pinot response we need to be able to mock out the ResultSet interface and hence we
  // create an interface
  // for the logic to determine the handling function based in the ResultSet class name. See usages
  // of resultSetTypePredicateProvider
  // to see how it used.
  private final ResultSetTypePredicateProvider resultSetTypePredicateProvider;
  private final PinotClientFactory pinotClientFactory;

  private final JsonFormat.Printer protoJsonPrinter =
      JsonFormat.printer().omittingInsignificantWhitespace();

  private Timer pinotQueryExecutionTimer;
  private int slowQueryThreshold = DEFAULT_SLOW_QUERY_THRESHOLD_MS;
  private String percentileAggFunction = DEFAULT_PERCENTILE_AGGREGATION_FUNCTION;

  public PinotBasedRequestHandler() {
    this(new DefaultResultSetTypePredicateProvider(), PinotClientFactory.get());
  }

  PinotBasedRequestHandler(
      ResultSetTypePredicateProvider resultSetTypePredicateProvider,
      PinotClientFactory pinotClientFactory) {
    this.resultSetTypePredicateProvider = resultSetTypePredicateProvider;
    this.pinotClientFactory = pinotClientFactory;
    this.pinotMapConverter = new PinotMapConverter();
  }

  private void initMetrics() {
    // Registry the latency metric with handler as a tag.
    this.pinotQueryExecutionTimer = PlatformMetricsRegistry.registerTimer(
        "pinot.query.latency", Map.of("handler", name), true);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void init(String name, Config config) {
    this.name = name;

    if (!config.hasPath(TENANT_COLUMN_NAME_CONFIG_KEY)) {
      throw new RuntimeException(TENANT_COLUMN_NAME_CONFIG_KEY +
          " is not defined in the " + name + " request handler.");
    }

    String tenantColumnName = config.getString(TENANT_COLUMN_NAME_CONFIG_KEY);
    this.viewDefinition = ViewDefinition.parse(
        config.getConfig(VIEW_DEFINITION_CONFIG_KEY), tenantColumnName);

    if (config.hasPath(PERCENTILE_AGGREGATION_FUNCTION_CONFIG)) {
      this.percentileAggFunction = config.getString(PERCENTILE_AGGREGATION_FUNCTION_CONFIG);
    }
    LOG.info("Using {} function for percentile aggregations of handler: {}",
        this.percentileAggFunction, name);

    this.request2PinotSqlConverter =
        new QueryRequestToPinotSQLConverter(viewDefinition, this.percentileAggFunction);

    if (config.hasPath(SLOW_QUERY_THRESHOLD_MS_CONFIG)) {
      this.slowQueryThreshold = config.getInt(SLOW_QUERY_THRESHOLD_MS_CONFIG);
    }
    LOG.info("Using {}ms as the threshold for logging slow queries of handler: {}",
        slowQueryThreshold, name);

    initMetrics();
  }

  /**
   * Returns a QueryCost that is an indication of whether the given query can be handled by this
   * handler and if so, how costly is it to handle that query.
   *
   * A query can usually be handled by Pinot handler if the Pinot view of this handler has all the
   * columns that are referenced in the incoming query. If the Pinot view is a filtered view on
   * some view column filters, the incoming query has to have those filters to match the view.
   */
  @Override
  public QueryCost canHandle(QueryRequest request, Set<String> referencedSources,
      ExecutionContext executionContext) {
    Set<String> referencedColumns = executionContext.getReferencedColumns();

    Preconditions.checkArgument(!referencedColumns.isEmpty());
    boolean found = true;
    for (String referencedColumn : referencedColumns) {
      if (!viewDefinition.containsColumn(referencedColumn)) {
        found = false;
        break;
      }
    }

    // If the view has any column filters, check if the query has those filters as **mandatory**
    // filters. If not, the view can't serve the query.
    Map<String, ViewColumnFilter> viewFilterMap = viewDefinition.getColumnFilterMap();
    if (found && !viewFilterMap.isEmpty()) {
        Set<String> columns = getMatchingViewFilterColumns(request.getFilter(), viewFilterMap);
        found = columns.equals(viewFilterMap.keySet());
    }

    // TODO: Come up with a way to compute the cost based on request and view definition
    // Higher columns --> Higher cost,
    // Finer the time granularity --> Higher the cost.
    return new QueryCost(found ? 0.5 : -1);
  }

  /**
   * Method to check if at least one Filter node matches all the view filters given in the
   * viewFilterMap.
   *
   * @return True if the given filter node matches all the view filters from the given map, False
   * otherwise.
   */
  private Set<String> getMatchingViewFilterColumns(Filter filter, Map<String,
      ViewColumnFilter> viewFilterMap) {
    // 1. Basic case: Filter is a leaf node. Check if the column exists in view filters and
    // return it.
    if (filter.getChildFilterCount() == 0) {
      return doesSingleViewFilterMatchLeafQueryFilter(viewFilterMap, filter) ?
          Set.of(filter.getLhs().getColumnIdentifier().getColumnName()) : Set.of();
    } else {
      // 2. Internal filter node. Recursively get the matching nodes from children.
      List<Set<String>> results = filter.getChildFilterList().stream()
          .map(f -> getMatchingViewFilterColumns(f, viewFilterMap)).collect(Collectors.toList());

      Set<String> result = results.get(0);
      for (Set<String> set : results.subList(1, results.size())) {
        // If the operation is OR, we need to get intersection of columns from all the children.
        // Otherwise, the operation should be AND and we can get union of all columns.
        result = filter.getOperator() == Operator.OR ? Sets.intersection(result, set) :
            Sets.union(result, set);
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
      Map<String, ViewColumnFilter> viewFilterMap, Filter queryFilter) {
    if (queryFilter.getLhs().getValueCase() != ValueCase.COLUMNIDENTIFIER) {
      return false;
    }
    if (queryFilter.getOperator() != Operator.IN && queryFilter.getOperator() != Operator.EQ) {
      return false;
    }

    String columnName = queryFilter.getLhs().getColumnIdentifier().getColumnName();
    ViewColumnFilter viewColumnFilter = viewFilterMap.get(columnName);
    if (viewColumnFilter == null) {
      return false;
    }

    switch (viewColumnFilter.getOperator()) {
      case IN:
        return isSubSet(viewColumnFilter.getValues(), queryFilter.getRhs());
      case EQ:
        return isEquals(viewColumnFilter.getValues(), queryFilter.getRhs());
      default:
        throw new IllegalArgumentException(
            "Unsupported view filter operator: " + viewColumnFilter.getOperator());
    }
  }

  /**
   * Checks that the values from the given expression are a subset of the given set.
   */
  private boolean isSubSet(Set<String> values, Expression expression) {
    if (!expression.hasLiteral()) {
      return false;
    }

    return values.containsAll(getExpressionValues(expression.getLiteral()));
  }

  /**
   * Checks that the values from the given expression are a subset of the given set.
   */
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
        expressionValues.addAll(value.getIntArrayList().stream().map(Object::toString).collect(
            Collectors.toSet()));
        break;
      case LONG:
        expressionValues.add(String.valueOf(value.getLong()));
        break;
      case LONG_ARRAY:
        expressionValues.addAll(value.getLongArrayList().stream().map(Object::toString).collect(
            Collectors.toSet()));
        break;
      case DOUBLE:
        expressionValues.add(String.valueOf(value.getDouble()));
        break;
      case DOUBLE_ARRAY:
        expressionValues.addAll(value.getDoubleArrayList().stream().map(Object::toString).collect(
            Collectors.toSet()));
        break;
      case FLOAT:
        expressionValues.add(String.valueOf(value.getFloat()));
        break;
      case FLOAT_ARRAY:
        expressionValues.addAll(value.getFloatArrayList().stream().map(Object::toString).collect(
            Collectors.toSet()));
        break;
      case BOOL:
        expressionValues.add(String.valueOf(value.getBoolean()).toLowerCase());
        break;
      case BOOLEAN_ARRAY:
        expressionValues.addAll(value.getBooleanArrayList().stream()
            .map(b -> b.toString().toLowerCase()).collect(Collectors.toSet()));
        break;
      default:
        // Ignore the rest of the types for now.
        throw new IllegalArgumentException("Unsupported value type in subset check.");
    }
    return expressionValues;
  }

  @Override
  public void handleRequest(
      ExecutionContext executionContext,
      QueryRequest request,
      QueryResultCollector<Row> collector) {
    long start = System.currentTimeMillis();
    validateQueryRequest(executionContext, request);

    // Rewrite the request filter after applying the view filters.
    if (!viewDefinition.getColumnFilterMap().isEmpty() &&
        !Filter.getDefaultInstance().equals(request.getFilter())) {
      request = rewriteRequestWithViewFiltersApplied(request,
          viewDefinition.getColumnFilterMap());
    }

    Entry<String, Params> pql =
        request2PinotSqlConverter
            .toSQL(executionContext, request, executionContext.getAllSelections());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to execute PQL: [ {} ] by RequestHandler: [ {} ]", pql, this.getName());
    }
    final PinotClient pinotClient = pinotClientFactory.getPinotClient(this.getName());

    final ResultSetGroup resultSetGroup;
    try {
      resultSetGroup = pinotQueryExecutionTimer.recordCallable(
          () -> pinotClient.executeQuery(pql.getKey(), pql.getValue()));
    } catch (Exception ex) {
      // Catch this exception to log the Pinot SQL query that caused the issue
      LOG.error("An error occurred while executing: {}", pql.getKey(), ex);
      // Rethrow for the caller to return an error.
      throw new RuntimeException(ex);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Query results: [ {} ]", resultSetGroup.toString());
    }
    // need to merge data especially for Pinot. That's why we need to track the map columns
    convert(resultSetGroup, collector, executionContext.getSelectedColumns());

    long requestTimeMs = System.currentTimeMillis() - start;
    if (requestTimeMs > slowQueryThreshold) {
      try {
        LOG.warn("Query Execution time: {} ms, sqlQuery: {}, queryRequest: {}",
            requestTimeMs, pql.getKey(), protoJsonPrinter.print(request));
      } catch (InvalidProtocolBufferException ignore) {
      }
    }
  }

  @Nonnull
  private QueryRequest rewriteRequestWithViewFiltersApplied(QueryRequest request,
      Map<String, ViewColumnFilter> columnFilterMap) {

    Filter newFilter = removeViewColumnFilter(request.getFilter(), columnFilterMap);
    QueryRequest.Builder builder = QueryRequest.newBuilder(request).clearFilter();
    if (!Filter.getDefaultInstance().equals(newFilter)) {
      builder.setFilter(newFilter);
    }
    return builder.build();
  }

  private Filter removeViewColumnFilter(Filter filter,
      Map<String, ViewColumnFilter> columnFilterMap) {
    if (filter.getChildFilterCount() > 0) {
      // Recursively try to remove the filter and eliminate the null nodes.
      Set<Filter> newFilters = filter.getChildFilterList().stream()
          .map(f -> removeViewColumnFilter(f, columnFilterMap))
          .filter(f -> !Filter.getDefaultInstance().equals(f))
          .collect(Collectors.toCollection(LinkedHashSet::new));

      if (newFilters.isEmpty()) {
        return Filter.getDefaultInstance();
      } else if (newFilters.size() == 1) {
        return newFilters.stream().findFirst().get();
      } else {
        return Filter.newBuilder(filter).clearChildFilter().addAllChildFilter(newFilters).build();
      }
    } else {
      return rewriteLeafFilter(filter, columnFilterMap);
    }
  }

  private Filter rewriteLeafFilter(Filter queryFilter,
      Map<String, ViewColumnFilter> columnFilterMap) {
    ViewColumnFilter viewColumnFilter =
        columnFilterMap.get(queryFilter.getLhs().getColumnIdentifier().getColumnName());
    if (viewColumnFilter == null) {
      return queryFilter;
    } else {

      // The only case where we need to return non-null filter is when view filter is based on 'IN'
      // and query filter is an 'EQ'
      if (viewColumnFilter.getOperator() == ViewColumnFilter.Operator.IN &&
          queryFilter.getOperator() == Operator.EQ) {
        return queryFilter;
      }

      return Filter.getDefaultInstance();
    }
  }

  void convert(
      ResultSetGroup resultSetGroup,
      QueryResultCollector<Row> collector,
      LinkedHashSet<String> selectedAttributes) {
    List<Row.Builder> rowBuilderList = new ArrayList<>();
    if (resultSetGroup.getResultSetCount() > 0) {
      ResultSet resultSet = resultSetGroup.getResultSet(0);
      // Pinot has different Response format for selection and aggregation/group by query.
      if (resultSetTypePredicateProvider.isSelectionResultSetType(resultSet)) {
        // map merging is only supported in the selection. Filtering and Group by has its own
        // syntax in Pinot
        handleSelection(resultSetGroup, rowBuilderList, selectedAttributes);
      } else if (resultSetTypePredicateProvider.isResultTableResultSetType(resultSet)) {
        handleTableFormatResultSet(resultSetGroup, rowBuilderList);
      } else {
        handleAggregationAndGroupBy(resultSetGroup, rowBuilderList);
      }
    }
    for (Row.Builder builder : rowBuilderList) {
      final Row row = builder.build();
      LOG.debug("collect a row: {}", row);
      collector.collect(row);
    }
    collector.finish();
  }

  private void handleSelection(
      ResultSetGroup resultSetGroup,
      List<Builder> rowBuilderList,
      LinkedHashSet<String> selectedAttributes) {
    int resultSetGroupCount = resultSetGroup.getResultSetCount();
    for (int i = 0; i < resultSetGroupCount; i++) {
      ResultSet resultSet = resultSetGroup.getResultSet(i);
      // Find the index in the result's column for each selected attributes
      PinotResultAnalyzer resultAnalyzer =
          PinotResultAnalyzer.create(resultSet, selectedAttributes, viewDefinition);

      // For each row returned from Pinot,
      // build the row according to the selected attributes from the request
      for (int rowId = 0; rowId < resultSet.getRowCount(); rowId++) {
        Builder builder;
        builder = Row.newBuilder();
        rowBuilderList.add(builder);

        // for each selected attributes in the request get the data from the
        // Pinot row result
        for (String logicalName : selectedAttributes) {
          // colVal will never be null. But getDataRow can throw a runtime exception if it failed
          // to retrieve data
          String colVal = resultAnalyzer.getDataFromRow(rowId, logicalName);
          builder.addColumn(Value.newBuilder().setString(colVal).build());
        }
      }
    }
  }

  private void handleAggregationAndGroupBy(
      ResultSetGroup resultSetGroup, List<Builder> rowBuilderList) {
    int resultSetGroupCount = resultSetGroup.getResultSetCount();
    Map<String, Integer> groupKey2RowIdMap = new HashMap<>();
    for (int i = 0; i < resultSetGroupCount; i++) {
      ResultSet resultSet = resultSetGroup.getResultSet(i);
      for (int rowId = 0; rowId < resultSet.getRowCount(); rowId++) {
        Builder builder;
        //
        int groupKeyLength = resultSet.getGroupKeyLength();
        String groupKey;
        StringBuilder groupKeyBuilder = new StringBuilder();
        String groupKeyDelim = "";
        for (int g = 0; g < groupKeyLength; g++) {
          String colVal = resultSet.getGroupKeyString(rowId, g);
          groupKeyBuilder.append(groupKeyDelim).append(colVal);
          groupKeyDelim = "|";
        }
        groupKey = groupKeyBuilder.toString();
        if (!groupKey2RowIdMap.containsKey(groupKey)) {
          builder = Row.newBuilder();
          rowBuilderList.add(builder);
          groupKey2RowIdMap.put(groupKey, rowId);
          for (int g = 0; g < groupKeyLength; g++) {
            String colVal = resultSet.getGroupKeyString(rowId, g);
            // add it only the first time
            builder.addColumn(Value.newBuilder().setString(colVal).build());
            groupKeyBuilder.append(colVal).append(groupKeyDelim);
            groupKeyDelim = "|";
          }
        } else {
          builder = rowBuilderList.get(groupKey2RowIdMap.get(groupKey));
        }
        int columnCount = resultSet.getColumnCount();
        if (columnCount > 0) {
          for (int c = 0; c < columnCount; c++) {
            String colVal = resultSet.getString(rowId, c);
            builder.addColumn(Value.newBuilder().setString(colVal).build());
          }
        }
      }
    }
  }

  private void handleTableFormatResultSet(
      ResultSetGroup resultSetGroup, List<Builder> rowBuilderList) {
    int resultSetGroupCount = resultSetGroup.getResultSetCount();
    for (int i = 0; i < resultSetGroupCount; i++) {
      ResultSet resultSet = resultSetGroup.getResultSet(i);
      for (int rowIdx = 0; rowIdx < resultSet.getRowCount(); rowIdx++) {
        Builder builder;
        builder = Row.newBuilder();
        rowBuilderList.add(builder);

        for (int colIdx = 0; colIdx < resultSet.getColumnCount(); colIdx++) {
          if (resultSet.getColumnName(colIdx).endsWith(ViewDefinition.MAP_KEYS_SUFFIX)) {
            // Read the key and value column values. The columns should be side by side. That's how
            // the Pinot query
            // is structured
            String mapKeys = resultSet.getString(rowIdx, colIdx);
            String mapVals = resultSet.getString(rowIdx, colIdx + 1);
            try {
              builder.addColumn(
                  Value.newBuilder().setString(pinotMapConverter.merge(mapKeys, mapVals)).build());
            } catch (IOException ex) {
              LOG.error("An error occurred while merging mapKeys and mapVals", ex);
              throw new RuntimeException(
                  "An error occurred while parsing the Pinot Table format response", ex);
            }
            // advance colIdx by 1 since we have read 2 columns
            colIdx++;
          } else {
            String val = resultSet.getString(rowIdx, colIdx);
            builder.addColumn(Value.newBuilder().setString(val).build());
          }
        }
      }
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
  }
}
