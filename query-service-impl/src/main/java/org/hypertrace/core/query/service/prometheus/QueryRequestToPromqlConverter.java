package org.hypertrace.core.query.service.prometheus;

import static org.hypertrace.core.query.service.QueryRequestUtil.getLogicalColumnNameForSimpleColumnExpression;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.QueryRequestUtil;
import org.hypertrace.core.query.service.QueryTimeRange;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Expression.ValueCase;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.prometheus.PrometheusViewDefinition.MetricConfig;

class QueryRequestToPromqlConverter {

  private final PrometheusViewDefinition prometheusViewDefinition;
  private final PrometheusFunctionConverter prometheusFunctionConverter;
  private final FilterToPromqlConverter filterToPromqlConverter;

  QueryRequestToPromqlConverter(PrometheusViewDefinition prometheusViewDefinition) {
    this.prometheusViewDefinition = prometheusViewDefinition;
    this.prometheusFunctionConverter = new PrometheusFunctionConverter();
    this.filterToPromqlConverter = new FilterToPromqlConverter();
  }

  PromQLInstantQuery convertToPromqlInstantQuery(
      ExecutionContext executionContext,
      QueryRequest request,
      LinkedHashSet<Expression> allSelections) {

    QueryTimeRange queryTimeRange =
        executionContext
            .getQueryTimeRange()
            .orElseThrow(() -> new RuntimeException("Time Range missing in query"));

    return new PromQLInstantQuery(
        buildPromqlQueries(
            request, allSelections, queryTimeRange, executionContext.getTimeFilterColumn()),
        queryTimeRange.getEndTime());
  }

  PromQLRangeQuery convertToPromqlRangeQuery(
      ExecutionContext executionContext,
      QueryRequest request,
      LinkedHashSet<Expression> allSelections) {

    QueryTimeRange queryTimeRange =
        executionContext
            .getQueryTimeRange()
            .orElseThrow(() -> new RuntimeException("Time Range missing in query"));

    return new PromQLRangeQuery(
        buildPromqlQueries(
            request, allSelections, queryTimeRange, executionContext.getTimeFilterColumn()),
        queryTimeRange.getStartTime(),
        queryTimeRange.getEndTime(),
        getTimeSeriesPeriod(executionContext));
  }

  private List<String> buildPromqlQueries(
      QueryRequest request,
      LinkedHashSet<Expression> allSelections,
      QueryTimeRange queryTimeRange,
      String timeFilterColumn) {
    List<String> groupByList = getGroupByList(request);

    List<String> filterList = new ArrayList<>();
    filterToPromqlConverter.convertFilterToString(
        request.getFilter(), filterList, timeFilterColumn, this::convertColumnAttributeToString);

    // iterate over all the functions in the query except for date time function (which is handled
    // separately and not a part of the query string)
    return allSelections.stream()
        .filter(
            expression ->
                expression.getValueCase().equals(ValueCase.FUNCTION)
                    && !QueryRequestUtil.isDateTimeFunction(expression))
        .map(
            functionExpression ->
                mapToPromqlQuery(functionExpression, groupByList, filterList, queryTimeRange))
        .collect(Collectors.toUnmodifiableList());
  }

  private String mapToPromqlQuery(
      Expression functionExpression,
      List<String> groupByList,
      List<String> filterList,
      QueryTimeRange queryTimeRange) {
    String functionName =
        prometheusFunctionConverter.mapToPrometheusFunctionName(functionExpression);
    MetricConfig metricConfig = getMetricConfigForFunction(functionExpression);
    return buildQuery(
        metricConfig.getName(),
        functionName,
        String.join(", ", groupByList),
        String.join(", ", filterList),
        queryTimeRange.getDuration().toMillis());
  }

  private List<String> getGroupByList(QueryRequest queryRequest) {
    return queryRequest.getGroupByList().stream()
        .filter(Predicate.not(QueryRequestUtil::isDateTimeFunction))
        .map(this::convertColumnAttributeToString)
        .collect(Collectors.toUnmodifiableList());
  }

  private Duration getTimeSeriesPeriod(ExecutionContext executionContext) {
    return executionContext.getTimeSeriesPeriod().get();
  }

  private String buildQuery(
      String metricName, String function, String groupByList, String filter, long durationMillis) {
    String template =
        "%s by (%s) (%s(%s{%s}[%sms]))"; // sum by (a1, a2) (sum_over_time(num_calls{a4="..",
    // a5=".."}[xms]))

    return String.format(
        template,
        function,
        groupByList,
        function + "_over_time", // assuming gauge type of metric
        metricName,
        filter,
        durationMillis);
  }

  private MetricConfig getMetricConfigForFunction(Expression functionSelection) {
    return prometheusViewDefinition.getMetricConfigForLogicalMetricName(
        getLogicalColumnNameForSimpleColumnExpression(
            functionSelection.getFunction().getArgumentsList().get(0)));
  }

  private String convertColumnAttributeToString(Expression expression) {
    return prometheusViewDefinition.getPhysicalColumnNameForLogicalColumnName(
        getLogicalColumnNameForSimpleColumnExpression(expression));
  }
}
