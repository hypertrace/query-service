package org.hypertrace.core.query.service.prometheus;

import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_AVG;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_COUNT;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_MAX;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_MIN;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_SUM;

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
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.prometheus.PrometheusViewDefinition.MetricConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class QueryRequestToPromqlConverter {

  private static final Logger LOG = LoggerFactory.getLogger(QueryRequestToPromqlConverter.class);

  private final PrometheusViewDefinition prometheusViewDefinition;

  QueryRequestToPromqlConverter(PrometheusViewDefinition prometheusViewDefinition) {
    this.prometheusViewDefinition = prometheusViewDefinition;
  }

  /**
   * 1. selection should be equal to group by list 2. number of function selection is equal to
   * number of query fired 3. count(*) type of query can't be served
   */
  PromqlQuery toPromql(
      ExecutionContext executionContext,
      QueryRequest request,
      LinkedHashSet<Expression> allSelections) {
    String groupByList = getGroupByList(request);

    List<String> filterList = new ArrayList<>();
    convertFilter2String(request.getFilter(), filterList);

    QueryTimeRange queryTimeRange =
        executionContext
            .getQueryTimeRange()
            .orElseThrow(() -> new RuntimeException("Time Range missing in query"));

    List<String> queries =
        allSelections.stream()
            .filter(expression -> expression.getValueCase().equals(ValueCase.FUNCTION))
            .map(
                functionExpression -> {
                  String functionName = getFunctionName(functionExpression);
                  MetricConfig metricConfig = getMetricConfigForFunction(functionExpression);
                  return buildQuery(
                      metricConfig.getName(),
                      functionName,
                      groupByList,
                      String.join(", ", filterList),
                      queryTimeRange.getDuration().toMillis());
                })
            .collect(Collectors.toUnmodifiableList());

    return new PromqlQuery(
        queries,
        executionContext.getTimeSeriesPeriod().isEmpty(),
        queryTimeRange.getStartTime(),
        queryTimeRange.getEndTime(),
        getStep(executionContext));
  }

  private String getGroupByList(QueryRequest queryRequest) {
    List<String> groupByList = Lists.newArrayList();
    if (queryRequest.getGroupByCount() > 0) {
      for (Expression expression : queryRequest.getGroupByList()) {
        groupByList.add(convertColumnIdentifierExpression2String(expression));
      }
    }
    return String.join(", ", groupByList);
  }

  private long getStep(ExecutionContext executionContext) {
    return executionContext.getTimeSeriesPeriod().map(Duration::toMillis).orElse(-1L);
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
        functionSelection.getFunction().getArgumentsList().get(0).getColumnIdentifier().getColumnName());
  }

  private String convertColumnIdentifierExpression2String(Expression expression) {
    String logicalColumnName = expression.getColumnIdentifier().getColumnName();
    return prometheusViewDefinition.getPhysicalColumnName(logicalColumnName);
  }

  /**
   * only `AND` operator in filter is allowed
   *
   * <p>rhs of leaf filter should be literal
   */
  private void convertFilter2String(Filter filter, List<String> filterList) {
    if (filter.getChildFilterCount() > 0) {
      for (Filter childFilter : filter.getChildFilterList()) {
        convertFilter2String(childFilter, filterList);
      }
    } else {
      if (filter.getLhs().getValueCase() == ValueCase.COLUMNIDENTIFIER &&
          QueryRequestUtil.isTimeColumn(filter.getLhs().getColumnIdentifier().getColumnName())) {
        return;
      }
      StringBuilder builder = new StringBuilder();
      builder.append(convertColumnIdentifierExpression2String(filter.getLhs()));
      switch (filter.getOperator()) {
        case IN:
        case EQ:
          builder.append("=");
          break;
        case NEQ:
          builder.append("!=");
          break;
        case LIKE:
          builder.append("=~");
          break;
        default:
          // throw exception
      }
      builder.append(convertLiteralToString(filter.getRhs().getLiteral()));
      filterList.add(builder.toString());
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
        ret = value.getString();
        break;
      case LONG:
        ret = String.valueOf(value.getLong());
        break;
      case INT:
        ret = String.valueOf(value.getInt());
        break;
      case FLOAT:
        ret = String.valueOf(value.getFloat());
        break;
      case DOUBLE:
        ret = String.valueOf(value.getDouble());
        break;
      case BYTES:
        ret = Hex.encodeHexString(value.getBytes().toByteArray());
        break;
      case BOOL:
        ret = String.valueOf(value.getBoolean());
        break;
      case TIMESTAMP:
        ret = String.valueOf(value.getTimestamp());
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
