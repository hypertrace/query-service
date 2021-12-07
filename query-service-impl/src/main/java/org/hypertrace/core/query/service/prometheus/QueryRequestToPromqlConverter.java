package org.hypertrace.core.query.service.prometheus;

import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_AVG;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_COUNT;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_MAX;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_MIN;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_SUM;
import static org.hypertrace.core.query.service.QueryRequestUtil.getLogicalColumnNameForSimpleColumnExpression;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Hex;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.QueryRequestUtil;
import org.hypertrace.core.query.service.QueryTimeRange;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Expression.ValueCase;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.prometheus.PrometheusViewDefinition.MetricConfig;

class QueryRequestToPromqlConverter {

  private final PrometheusViewDefinition prometheusViewDefinition;

  QueryRequestToPromqlConverter(PrometheusViewDefinition prometheusViewDefinition) {
    this.prometheusViewDefinition = prometheusViewDefinition;
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
        buildPromqlQueries(request, allSelections, queryTimeRange), queryTimeRange.getEndTime());
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
        buildPromqlQueries(request, allSelections, queryTimeRange),
        queryTimeRange.getStartTime(),
        queryTimeRange.getEndTime(),
        getTimeSeriesPeriod(executionContext));
  }

  private List<String> buildPromqlQueries(
      QueryRequest request,
      LinkedHashSet<Expression> allSelections,
      QueryTimeRange queryTimeRange) {
    List<String> groupByList = getGroupByList(request);

    List<String> filterList = new ArrayList<>();
    convertFilterToString(request.getFilter(), filterList);

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
    String functionName = getFunctionName(functionExpression);
    MetricConfig metricConfig = getMetricConfigForFunction(functionExpression);
    return buildQuery(
        metricConfig.getName(),
        functionName,
        String.join(", ", groupByList),
        String.join(", ", filterList),
        queryTimeRange.getDuration().toMillis());
  }

  private List<String> getGroupByList(QueryRequest queryRequest) {
    List<String> groupByList = Lists.newArrayList();
    if (queryRequest.getGroupByCount() > 0) {
      for (Expression expression : queryRequest.getGroupByList()) {
        // skip datetime function in group-by
        if (QueryRequestUtil.isDateTimeFunction(expression)) {
          continue;
        }
        groupByList.add(convertColumnAttributeToString(expression));
      }
    }
    return groupByList;
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

  private String getFunctionName(Expression functionSelection) {
    switch (functionSelection.getFunction().getFunctionName().toUpperCase()) {
      case QUERY_FUNCTION_SUM:
        return "sum";
      case QUERY_FUNCTION_MAX:
        return "max";
      case QUERY_FUNCTION_MIN:
        return "min";
      case QUERY_FUNCTION_AVG:
        return "avg";
      case QUERY_FUNCTION_COUNT:
        return "count";
      default:
        throw new RuntimeException("");
    }
  }

  private MetricConfig getMetricConfigForFunction(Expression functionSelection) {
    return prometheusViewDefinition.getMetricConfig(
        getLogicalColumnNameForSimpleColumnExpression(
            functionSelection.getFunction().getArgumentsList().get(0)));
  }

  private String convertColumnAttributeToString(Expression expression) {
    return prometheusViewDefinition.getPhysicalColumnName(
        getLogicalColumnNameForSimpleColumnExpression(expression));
  }

  /**
   * only `AND` operator in filter is allowed
   *
   * <p>rhs of leaf filter should be literal
   */
  private void convertFilterToString(Filter filter, List<String> filterList) {
    if (filter.getChildFilterCount() > 0) {
      for (Filter childFilter : filter.getChildFilterList()) {
        convertFilterToString(childFilter, filterList);
      }
    } else {
      if (QueryRequestUtil.isSimpleColumnExpression(filter.getLhs())
          && QueryRequestUtil.isTimeColumn(
              getLogicalColumnNameForSimpleColumnExpression(filter.getLhs()))) {
        return;
      }
      StringBuilder builder = new StringBuilder();
      builder.append(convertColumnAttributeToString(filter.getLhs()));
      builder.append(convertOperatorToString(filter.getOperator()));
      builder.append(convertLiteralToString(filter.getRhs().getLiteral()));
      filterList.add(builder.toString());
    }
  }

  private String convertOperatorToString(Operator operator) {
    switch (operator) {
      case IN:
      case EQ:
        return "=";
      case NEQ:
        return "!=";
      case LIKE:
        return "=~";
      default:
        throw new RuntimeException(
            String.format("Equivalent operator %s not supported in promql", operator));
    }
  }

  private String convertLiteralToString(LiteralConstant literal) {
    Value value = literal.getValue();
    String ret = null;
    switch (value.getValueType()) {
      case STRING_ARRAY:
        StringBuilder builder = new StringBuilder("\"");
        for (String item : value.getStringArrayList()) {
          if (builder.length() > 1) {
            builder.append("|");
          }
          builder.append(item);
        }
        builder.append("\"");
        ret = builder.toString();
        break;
      case BYTES_ARRAY:
        builder = new StringBuilder("\"");
        for (ByteString item : value.getBytesArrayList()) {
          if (builder.length() > 1) {
            builder.append("|");
          }
          builder.append(Hex.encodeHexString(item.toByteArray()));
        }
        builder.append("\"");
        ret = builder.toString();
        break;
      case STRING:
        ret = "\"" + value.getString() + "\"";
        break;
      case LONG:
        ret = "\"" + value.getLong() + "\"";
        break;
      case INT:
        ret = "\"" + value.getInt() + "\"";
        break;
      case FLOAT:
        ret = "\"" + value.getFloat() + "\"";
        break;
      case DOUBLE:
        ret = "\"" + value.getDouble() + "\"";
        break;
      case BYTES:
        ret = "\"" + Hex.encodeHexString(value.getBytes().toByteArray()) + "\"";
        break;
      case BOOL:
        ret = "\"" + value.getBoolean() + "\"";
        break;
      case TIMESTAMP:
        ret = "\"" + value.getTimestamp() + "\"";
        break;
      case NULL_NUMBER:
        ret = "0";
        break;
      case NULL_STRING:
        ret = "null";
        break;
      case LONG_ARRAY:
      case INT_ARRAY:
      case FLOAT_ARRAY:
      case DOUBLE_ARRAY:
      case BOOLEAN_ARRAY:
      case UNRECOGNIZED:
        break;
    }
    return ret;
  }
}
