package org.hypertrace.core.query.service.pinot;

import com.google.common.base.Preconditions;
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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.client.ResultSetGroup;
import org.hypertrace.core.query.service.QueryContext;
import org.hypertrace.core.query.service.QueryCost;
import org.hypertrace.core.query.service.QueryResultCollector;
import org.hypertrace.core.query.service.RequestAnalyzer;
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

  @Override
  public QueryCost canHandle(QueryRequest request, Set<String> referencedSources,
      RequestAnalyzer analyzer) {
    Set<String> referencedColumns = analyzer.getReferencedColumns();

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
    Map<String, ViewColumnFilter> filterMap = viewDefinition.getColumnFilterMap();
    if (found && !filterMap.isEmpty()) {
      Map<String, ViewColumnFilter> viewFilterMap = viewDefinition.getColumnFilterMap();

      // Check for the presence of every view filter column in query's referenced columns
      // so that we can terminate early.
      for (Map.Entry<String, ViewColumnFilter> entry : viewFilterMap.entrySet()) {
        if (!analyzer.getReferencedColumns().contains(entry.getKey())) {
          found = false;
          break;
        }
      }

      if (!found) {
        return new QueryCost(-1);
      }

      found = doViewFiltersMatch(analyzer.getOptimizedFilter(), viewFilterMap);
    }

    // TODO: Come up with a way to compute the cost based on request and view definition
    // Higher columns --> Higher cost,
    // Finer the time granularity --> Higher the cost.
    return new QueryCost(found ? 0.5 : -1);
  }

  private boolean doViewFiltersMatch(Filter filter, Map<String, ViewColumnFilter> viewFilterMap) {
    // Navigate the entire filter tree to check if every query filter which has the
    // view filter columns has all matching view filters. If not, return false since
    // this view can't handle such query.
    if (filter.getChildFilterCount() == 0) {
      if (viewFilterMap.size() != 1) {
        return false;
      }

      // If the column names of query filter and view filter match but the filters
      // don't match, we can exit here.
      String viewFilterColumnName = viewFilterMap.keySet().stream().findFirst().get();
      if (filter.getLhs().getValueCase() == ValueCase.COLUMNIDENTIFIER &&
          StringUtils.equals(viewFilterColumnName,
              filter.getLhs().getColumnIdentifier().getColumnName()))
      {
        return doesSingleViewFilterMatchLeafQueryFilter(viewFilterMap.get(viewFilterColumnName),
            filter);
      }

      return false;
    }

    Map<String, Filter> childLeafFilterMap = filter.getChildFilterList().stream()
        .filter(f -> f.getChildFilterCount() == 0 &&
            f.getLhs().getValueCase() == Expression.ValueCase.COLUMNIDENTIFIER)
        .filter(
            f -> viewFilterMap.containsKey(f.getLhs().getColumnIdentifier().getColumnName()))
        .collect(Collectors
            .toMap(f -> f.getLhs().getColumnIdentifier().getColumnName(), f -> f));
    if (childLeafFilterMap.isEmpty()) {
      long matchCount = filter.getChildFilterList().stream()
          .map(f -> doViewFiltersMatch(f, viewFilterMap))
          .filter(b -> b).count();
      if (filter.getOperator() == Operator.AND && matchCount > 0) {
        return true;
      }

      // When children are being OR'ed, all the children must match the view filters.
      return filter.getOperator() == Operator.OR && matchCount == filter.getChildFilterCount();
    }

    if (filter.getOperator() != Operator.AND) return false;

    for (Map.Entry<String, ViewColumnFilter> entry : viewFilterMap.entrySet()) {
      // Make sure all view filters match.
      Filter queryFilter = childLeafFilterMap.get(entry.getKey());
      Objects.requireNonNull(queryFilter);
      if (!doesSingleViewFilterMatchLeafQueryFilter(entry.getValue(), queryFilter)) {
        // Few columns matched but others didn't match.
        return false;
      }
    }

    // Perfect match.
    return true;
  }

  /**
   * Method to check if the given ViewColumnFilter matches the given query filter. A match here
   * means the view column is superset of what the query filter is looking for, need not be an exact
   * match.
   */
  private boolean doesSingleViewFilterMatchLeafQueryFilter(ViewColumnFilter viewColumnFilter, Filter queryFilter) {
    if (queryFilter.getOperator() != Operator.IN && queryFilter.getOperator() != Operator.EQ) {
      return false;
    }

    switch(viewColumnFilter.getOperator()) {
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
      QueryContext queryContext,
      QueryRequest request,
      QueryResultCollector<Row> collector,
      RequestAnalyzer requestAnalyzer) {
    long start = System.currentTimeMillis();
    validateQueryRequest(queryContext, request);

    // Remove the filters which match the view column filters because they shouldn't be passed down
    // to Pinot.
    if (!viewDefinition.getColumnFilterMap().isEmpty()) {
      request = removeViewColumnFilters(request, viewDefinition.getColumnFilterMap());
    }

    Entry<String, Params> pql =
        request2PinotSqlConverter.toSQL(queryContext, request, requestAnalyzer.getAllSelections());
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
    convert(resultSetGroup, collector, requestAnalyzer.getSelectedColumns());

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
  private QueryRequest removeViewColumnFilters(QueryRequest request,
      Map<String, ViewColumnFilter> columnFilterMap) {

    Filter filter = request.getFilter();
    for (String columnName : columnFilterMap.keySet()) {
      filter = removeViewColumnFilter(filter, columnName);
      if (filter == null) {
        break;
      }
    }

    QueryRequest.Builder builder = QueryRequest.newBuilder(request);
    if (filter == null) {
      return builder.clearFilter().build();
    } else {
      return builder.clearFilter().setFilter(filter).build();
    }
  }

  @Nullable
  private Filter removeViewColumnFilter(Filter filter, String columnName) {
    if (filter.getChildFilterCount() > 0) {
      // Recursively try to remove the filter and eliminate the null nodes.
      List<Filter> newFilters = filter.getChildFilterList().stream()
          .map(f -> removeViewColumnFilter(f, columnName))
          .filter(Objects::nonNull).collect(Collectors.toList());
      if (newFilters.size() == 1) {
        return newFilters.get(0);
      } else {
        return Filter.newBuilder(filter).clearChildFilter().addAllChildFilter(newFilters).build();
      }
    } else {
      if (filter.getLhs().getColumnIdentifier().getColumnName().equals(columnName)) {
        return null;
      }
      return filter;
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

  private void validateQueryRequest(QueryContext queryContext, QueryRequest request) {
    // Validate QueryContext and tenant id presence
    Preconditions.checkNotNull(queryContext);
    Preconditions.checkNotNull(queryContext.getTenantId());

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
