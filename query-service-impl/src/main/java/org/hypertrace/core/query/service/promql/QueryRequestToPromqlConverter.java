package org.hypertrace.core.query.service.promql;

import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_AVG;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_MAX;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_MIN;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_SUM;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Hex;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.QueryTimeRange;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Expression.ValueCase;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.pinot.ViewDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class QueryRequestToPromqlConverter {

  private static final Logger LOG = LoggerFactory.getLogger(QueryRequestToPromqlConverter.class);

  private final ViewDefinition viewDefinition;
  private final Joiner joiner = Joiner.on(", ").skipNulls();

  QueryRequestToPromqlConverter(
      ViewDefinition viewDefinition) {
    this.viewDefinition = viewDefinition;
  }

  /**
   * 1. selection should be equal to group by list
   * 2. number of function selection is equal to number of query fired
   * 3. count(*) type of query can't be served
   */
  PromqlQuery toPromql(
      ExecutionContext executionContext,
      QueryRequest request,
      LinkedHashSet<Expression> allSelections) {
    String groupByList = getGroupByList(request);

    StringBuilder filter = new StringBuilder();
    convertFilter2String(request.getFilter(), filter);


    QueryTimeRange queryTimeRange = executionContext.getQueryTimeRange().orElseThrow(
        () -> new RuntimeException("Time Range missing in query"));

    List<String> queries = allSelections.stream().filter(expression -> expression.getValueCase().equals(ValueCase.FUNCTION))
        .map(functionExpression -> {
          String functionName = getFunctionName(functionExpression);
          String metricName = getMetricNameFromFunction(functionExpression);
          return buildQuery(metricName, functionName, groupByList, filter.toString(), queryTimeRange.getDuration().toMillis());
        }).collect(Collectors.toUnmodifiableList());

    return new PromqlQuery(
        queries, executionContext.getTimeSeriesPeriod().isEmpty(),
        queryTimeRange.getStartTime(), queryTimeRange.getEndTime(),
        getStep(executionContext));
  }

  private String getGroupByList(QueryRequest queryRequest) {
    List<String> groupByList = Lists.newArrayList();
    if (queryRequest.getGroupByCount() > 0) {
      for (Expression expression : queryRequest.getGroupByList()) {
        groupByList.add(convertColumnIdentifierExpression2String(expression));
      }
    }
    return String.join(",", groupByList);
  }

  private long getStep(ExecutionContext executionContext) {
    return executionContext
        .getTimeSeriesPeriod().map(Duration::toMillis)
        .orElse(-1L);
  }

  private String buildQuery(
      String metricName, String function,
      String groupByList, String filter, long durationMillis) {
    String template = "%s by (%s) (%s(%s{%s}[%sms]))"; // sum by (a1, a2) (sum_over_time(num_calls{a4="..", a5=".."}[xms]))

    return String.format(template, function, groupByList, function + "_over_time", metricName, filter, durationMillis);
  }

  private String getFunctionName(
      Expression functionSelection) {
    switch (functionSelection.getFunction().getFunctionName().toUpperCase()) {
      case QUERY_FUNCTION_SUM:
        return "sum";
      case QUERY_FUNCTION_MAX:
        return "max";
      case QUERY_FUNCTION_MIN:
        return "min";
      case QUERY_FUNCTION_AVG:
        return "avg";
      default:
        throw new RuntimeException("");
    }
  }

  private String getMetricNameFromFunction(
      Expression functionSelection) {
    // todo map columnName
    return functionSelection.getColumnIdentifier().getColumnName();
  }

  private String convertColumnIdentifierExpression2String(
      Expression expression) {
    String logicalColumnName = expression.getColumnIdentifier().getColumnName();
    List<String> columnNames = viewDefinition.getPhysicalColumnNames(logicalColumnName);
    return joiner.join(columnNames);
  }

  /**
   * only `AND` operator in filter is allowed
   *
   * rhs of leaf filter should be literal
   */
  private void convertFilter2String(Filter filter, StringBuilder builder) {
    if (filter.getChildFilterCount() > 0) {
      String delim = ",";
      for (Filter childFilter : filter.getChildFilterList()) {
        builder.append(delim);
        convertFilter2String(childFilter, builder);
        builder.append(" ");
      }
    } else {
      builder.append(
          convertColumnIdentifierExpression2String(filter.getLhs()));
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