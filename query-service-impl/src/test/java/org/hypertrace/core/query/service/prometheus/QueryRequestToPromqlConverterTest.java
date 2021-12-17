package org.hypertrace.core.query.service.prometheus;

import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createColumnExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createFunctionExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createInFilter;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createTimeColumnGroupByExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createTimeFilter;
import static org.hypertrace.core.query.service.QueryRequestUtil.createStringLiteralValueExpression;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.QueryRequest.Builder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class QueryRequestToPromqlConverterTest {

  @Test
  void testInstantQueryWithGroupByWithMultipleAggregates() {
    QueryRequest query = buildMultipleGroupByMultipleAggQuery();
    Builder builder = QueryRequest.newBuilder(query);
    builder.setLimit(20);
    PrometheusViewDefinition prometheusViewDefinition =
        PrometheusTestUtils.getDefaultPrometheusViewDefinition();

    QueryRequest queryRequest = builder.build();

    ExecutionContext executionContext = new ExecutionContext("__default", queryRequest);
    executionContext.setTimeFilterColumn("SERVICE.startTime");
    Map<String, String> metricNameToQueryMap = new LinkedHashMap<>();
    PromQLInstantQueries promqlQuery =
        new QueryRequestToPromqlConverter(prometheusViewDefinition)
            .convertToPromqlInstantQuery(
                executionContext,
                builder.build(),
                createSelectionsFromQueryRequest(queryRequest),
                metricNameToQueryMap);

    // time filter is removed from the query
    String query1 =
        "count by (service_name, api_name) (count_over_time(error_count{tenant_id=\"__default\"}[100ms]))";
    String query2 =
        "avg by (service_name, api_name) (avg_over_time(num_calls{tenant_id=\"__default\"}[100ms]))";

    Assertions.assertTrue(metricNameToQueryMap.containsValue(query1));
    Assertions.assertTrue(metricNameToQueryMap.containsValue(query2));
  }

  @Test
  void testInstantQueryWithGroupByWithMultipleAggregatesWithMultipleFilters() {
    QueryRequest query = buildMultipleGroupByMultipleAggQueryWithMultipleFilters();
    Builder builder = QueryRequest.newBuilder(query);
    builder.setLimit(20);
    PrometheusViewDefinition prometheusViewDefinition =
        PrometheusTestUtils.getDefaultPrometheusViewDefinition();

    QueryRequest queryRequest = builder.build();

    ExecutionContext executionContext = new ExecutionContext("__default", queryRequest);
    executionContext.setTimeFilterColumn("SERVICE.startTime");
    Map<String, String> metricNameToQueryMap = new LinkedHashMap<>();
    PromQLInstantQueries promqlQuery =
        new QueryRequestToPromqlConverter(prometheusViewDefinition)
            .convertToPromqlInstantQuery(
                executionContext,
                builder.build(),
                createSelectionsFromQueryRequest(queryRequest),
                metricNameToQueryMap);

    // time filter is removed from the query
    String query1 =
        "count by (service_name, api_name) (count_over_time(error_count{tenant_id=\"__default\", service_id=\"1|2|3\", service_name=~\"someregex\"}[100ms]))";
    String query2 =
        "avg by (service_name, api_name) (avg_over_time(num_calls{tenant_id=\"__default\", service_id=\"1|2|3\", service_name=~\"someregex\"}[100ms]))";

    Assertions.assertTrue(metricNameToQueryMap.containsValue(query1));
    Assertions.assertTrue(metricNameToQueryMap.containsValue(query2));
  }

  @Test
  void testTimeSeriesQueryWithGroupByWithMultipleAggregatesWithMultipleFilters() {
    QueryRequest query = buildMultipleGroupByMultipleAggQueryWithMultipleFiltersAndDateTime();
    Builder builder = QueryRequest.newBuilder(query);
    builder.setLimit(20);
    PrometheusViewDefinition prometheusViewDefinition =
        PrometheusTestUtils.getDefaultPrometheusViewDefinition();

    QueryRequest queryRequest = builder.build();

    ExecutionContext executionContext = new ExecutionContext("__default", queryRequest);
    executionContext.setTimeFilterColumn("SERVICE.startTime");
    Map<String, String> metricNameToQueryMap = new LinkedHashMap<>();
    PromQLRangeQueries promqlQuery =
        new QueryRequestToPromqlConverter(prometheusViewDefinition)
            .convertToPromqlRangeQuery(
                executionContext,
                builder.build(),
                createSelectionsFromQueryRequest(queryRequest),
                metricNameToQueryMap);

    // time filter is removed from the query
    String query1 =
        "count by (service_name, api_name) (count_over_time(error_count{tenant_id=\"__default\", service_id=\"1|2|3\", service_name=~\"someregex\"}[10ms]))";
    String query2 =
        "avg by (service_name, api_name) (avg_over_time(num_calls{tenant_id=\"__default\", service_id=\"1|2|3\", service_name=~\"someregex\"}[10ms]))";

    Assertions.assertTrue(metricNameToQueryMap.containsValue(query1));
    Assertions.assertTrue(metricNameToQueryMap.containsValue(query2));
    Assertions.assertEquals(10, promqlQuery.getPeriod().toMillis());
  }

  private QueryRequest buildMultipleGroupByMultipleAggQuery() {
    Builder builder = QueryRequest.newBuilder();
    builder.addAggregation(
        createFunctionExpression("Count", createColumnExpression("SERVICE.errorCount").build()));
    Expression avg =
        createFunctionExpression("AVG", createColumnExpression("SERVICE.numCalls").build());
    builder.addAggregation(avg);

    Filter startTimeFilter = createTimeFilter("SERVICE.startTime", Operator.GT, 100L);
    Filter endTimeFilter = createTimeFilter("SERVICE.startTime", Operator.LT, 200L);

    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .build();
    builder.setFilter(andFilter);

    builder.addGroupBy(createColumnExpression("SERVICE.name"));
    builder.addGroupBy(createColumnExpression("API.name"));
    return builder.build();
  }

  private QueryRequest buildMultipleGroupByMultipleAggQueryWithMultipleFilters() {
    Builder builder = QueryRequest.newBuilder();
    builder.addAggregation(
        createFunctionExpression("Count", createColumnExpression("SERVICE.errorCount").build()));
    Expression avg =
        createFunctionExpression("AVG", createColumnExpression("SERVICE.numCalls").build());
    builder.addAggregation(avg);

    Filter startTimeFilter = createTimeFilter("SERVICE.startTime", Operator.GT, 100L);
    Filter endTimeFilter = createTimeFilter("SERVICE.startTime", Operator.LT, 200L);
    Filter inFilter = createInFilter("SERVICE.id", List.of("1", "2", "3"));
    Filter likeFilter =
        Filter.newBuilder()
            .setOperator(Operator.LIKE)
            .setLhs(createColumnExpression("SERVICE.name").build())
            .setRhs(createStringLiteralValueExpression("someregex"))
            .build();
    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .addChildFilter(inFilter)
            .addChildFilter(likeFilter)
            .build();
    builder.setFilter(andFilter);

    builder.addGroupBy(createColumnExpression("SERVICE.name"));
    builder.addGroupBy(createColumnExpression("API.name"));
    return builder.build();
  }

  private QueryRequest buildMultipleGroupByMultipleAggQueryWithMultipleFiltersAndDateTime() {
    Builder builder = QueryRequest.newBuilder();
    builder.addAggregation(
        createFunctionExpression("Count", createColumnExpression("SERVICE.errorCount").build()));
    Expression avg =
        createFunctionExpression("AVG", createColumnExpression("SERVICE.numCalls").build());
    builder.addAggregation(avg);

    Filter startTimeFilter = createTimeFilter("SERVICE.startTime", Operator.GT, 100L);
    Filter endTimeFilter = createTimeFilter("SERVICE.startTime", Operator.LT, 200L);
    Filter inFilter = createInFilter("SERVICE.id", List.of("1", "2", "3"));
    Filter likeFilter =
        Filter.newBuilder()
            .setOperator(Operator.LIKE)
            .setLhs(createColumnExpression("SERVICE.name").build())
            .setRhs(createStringLiteralValueExpression("someregex"))
            .build();
    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .addChildFilter(inFilter)
            .addChildFilter(likeFilter)
            .build();
    builder.setFilter(andFilter);

    builder.addGroupBy(createColumnExpression("SERVICE.name"));
    builder.addGroupBy(createColumnExpression("API.name"));
    builder.addGroupBy(createTimeColumnGroupByExpression("SERVICE.startTime", "10:MILLISECONDS"));
    return builder.build();
  }

  private LinkedHashSet<Expression> createSelectionsFromQueryRequest(QueryRequest queryRequest) {
    LinkedHashSet<Expression> selections = new LinkedHashSet<>();

    selections.addAll(queryRequest.getGroupByList());
    selections.addAll(queryRequest.getSelectionList());
    selections.addAll(queryRequest.getAggregationList());

    return selections;
  }
}
