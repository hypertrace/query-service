package org.hypertrace.core.query.service.pinot;

import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.client.ResultSetGroup;
import org.hypertrace.core.query.service.QueryContext;
import org.hypertrace.core.query.service.QueryCost;
import org.hypertrace.core.query.service.QueryResultCollector;
import org.hypertrace.core.query.service.RequestAnalyzer;
import org.hypertrace.core.query.service.RequestHandler;
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

  public static String VIEW_DEFINITION_CONFIG_KEY = "viewDefinition";
  private static final int SLOW_REQUEST_THRESHOLD_MS = 3000; // A 3 seconds request is too slow

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

  private Timer pinotQueryExecutionTimer;

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
    this.pinotQueryExecutionTimer = new Timer();
    PlatformMetricsRegistry.register(
        String.format("pinot.%s.query.execution", PinotUtils.getMetricName(name)),
        pinotQueryExecutionTimer);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void init(String name, Map<String, Object> config) {
    this.name = name;
    // TODO:use typesafe HOCON object
    this.viewDefinition = (ViewDefinition) config.get(VIEW_DEFINITION_CONFIG_KEY);
    request2PinotSqlConverter = new QueryRequestToPinotSQLConverter(viewDefinition);
    initMetrics();
  }

  @Override
  public QueryCost canHandle(
      QueryRequest request, Set<String> referencedSources, Set<String> referencedColumns) {
    double cost = -1;
    boolean found = true;
    for (String referencedColumn : referencedColumns) {
      if (!viewDefinition.containsColumn(referencedColumn)) {
        found = false;
        break;
      }
    }
    // successfully found a view that can handle the request
    if (found) {
      // TODO: Come up with a way to compute the cost based on request and view definition
      // Higher columns --> Higher cost,
      // Finer the time granularity --> Higher the cost.
      cost = 0.5;
    }
    QueryCost queryCost = new QueryCost();
    queryCost.setCost(cost);
    return queryCost;
  }

  @Override
  public void handleRequest(
      QueryContext queryContext,
      QueryRequest request,
      QueryResultCollector<Row> collector,
      RequestAnalyzer requestAnalyzer) {
    long start = System.currentTimeMillis();
    validateQueryRequest(queryContext, request);
    Entry<String, Params> pql =
        request2PinotSqlConverter.toSQL(queryContext, request, requestAnalyzer.getAllSelections());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to execute PQL: [ {} ] by RequestHandler: [ {} ]", pql, this.getName());
    }
    final PinotClient pinotClient = pinotClientFactory.getPinotClient(this.getName());
    Context timerContext = pinotQueryExecutionTimer.time();
    try {
      final ResultSetGroup resultSetGroup = pinotClient.executeQuery(pql.getKey(), pql.getValue());
      timerContext.stop();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Query results: [ {} ]", resultSetGroup.toString());
      }
      // need to merge data especially for Pinot. That's why we need to track the map columns
      convert(resultSetGroup, collector, requestAnalyzer.getSelectedColumns());
      long requestTimeMs = System.currentTimeMillis() - start;
      if (requestTimeMs > SLOW_REQUEST_THRESHOLD_MS) {
        LOG.warn("Query Execution time: {} millis\nQuery Request: {}", requestTimeMs, request);
      }
    } catch (Exception ex) {
      // stop any timer context
      timerContext.stop();
      // Catch this exception to log the Pinot SQL query that caused the issue
      LOG.error("An error occurred while executing: {}", pql.getKey(), ex);
      // Rethrow for the caller to return an error.
      throw ex;
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
              LOG.error("An error occured while merging mapKeys and mapVals", ex);
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
