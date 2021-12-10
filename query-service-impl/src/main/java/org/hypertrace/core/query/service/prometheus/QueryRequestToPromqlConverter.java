package org.hypertrace.core.query.service.prometheus;

import static org.hypertrace.core.query.service.QueryRequestUtil.getLogicalColumnName;

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

  PromQLInstantQueries convertToPromqlInstantQuery(
      ExecutionContext executionContext,
      QueryRequest request,
      LinkedHashSet<Expression> allSelections) {
    QueryTimeRange queryTimeRange = getQueryTimeRange(executionContext);
    return new PromQLInstantQueries(
        buildPromqlQueries(
            executionContext.getTenantId(),
            request,
            allSelections,
            queryTimeRange.getDuration(),
            executionContext.getTimeFilterColumn()),
        queryTimeRange.getEndTime());
  }

  PromQLRangeQueries convertToPromqlRangeQuery(
      ExecutionContext executionContext,
      QueryRequest request,
      LinkedHashSet<Expression> allSelections) {
    QueryTimeRange queryTimeRange = getQueryTimeRange(executionContext);
    return new PromQLRangeQueries(
        buildPromqlQueries(
            executionContext.getTenantId(),
            request,
            allSelections,
            executionContext.getTimeSeriesPeriod().get(),
            executionContext.getTimeFilterColumn()),
        queryTimeRange.getStartTime(),
        queryTimeRange.getEndTime(),
        getTimeSeriesPeriod(executionContext));
  }

  private QueryTimeRange getQueryTimeRange(ExecutionContext executionContext) {
    return executionContext
        .getQueryTimeRange()
        .orElseThrow(() -> new RuntimeException("Time Range missing in query"));
  }

  private List<String> buildPromqlQueries(
      String tenantId,
      QueryRequest request,
      LinkedHashSet<Expression> allSelections,
      Duration duration,
      String timeFilterColumn) {
    List<String> groupByList = getGroupByList(request);

    List<String> filterList = new ArrayList<>();
    filterList.add(
        String.format("%s=\"%s\"", prometheusViewDefinition.getTenantAttributeName(), tenantId));
    filterToPromqlConverter.convertFilterToString(
        request.getFilter(), timeFilterColumn, this::convertColumnAttributeToString, filterList);

    // iterate over all the functions in the query except for date time function (which is handled
    // separately and not a part of the query string)
    return allSelections.stream()
        .filter(
            expression ->
                expression.getValueCase().equals(ValueCase.FUNCTION)
                    && !QueryRequestUtil.isDateTimeFunction(expression))
        .map(
            functionExpression ->
                mapToPromqlQuery(functionExpression, groupByList, filterList, duration))
        .collect(Collectors.toUnmodifiableList());
  }

  private String mapToPromqlQuery(
      Expression functionExpression,
      List<String> groupByList,
      List<String> filterList,
      Duration duration) {
    String functionName =
        prometheusFunctionConverter.mapToPrometheusFunctionName(functionExpression);
    MetricConfig metricConfig = getMetricConfigForFunction(functionExpression);
    return buildQuery(
        metricConfig.getMetricName(),
        functionName,
        String.join(", ", groupByList),
        String.join(", ", filterList),
        duration.toMillis());
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

  /**
   * Builds a promql query. example query `sum by (a1, a2) (sum_over_time(num_calls{a4="..",
   * a5=".."}[xms]))`
   */
  private String buildQuery(
      String metricName, String function, String groupByList, String filter, long durationMillis) {
    String template = "%s by (%s) (%s(%s{%s}[%sms]))";

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
        getLogicalColumnName(functionSelection.getFunction().getArgumentsList().get(0)));
  }

  private String convertColumnAttributeToString(Expression expression) {
    return prometheusViewDefinition.getPhysicalColumnNameForLogicalColumnName(
        getLogicalColumnName(expression));
  }
}
