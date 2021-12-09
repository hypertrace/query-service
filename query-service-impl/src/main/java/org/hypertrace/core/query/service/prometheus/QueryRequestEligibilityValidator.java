package org.hypertrace.core.query.service.prometheus;

import static org.hypertrace.core.query.service.QueryRequestUtil.getLogicalColumnNameForSimpleColumnExpression;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.QueryCost;
import org.hypertrace.core.query.service.QueryRequestUtil;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Expression.ValueCase;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.prometheus.PrometheusViewDefinition.MetricConfig;
import org.hypertrace.core.query.service.prometheus.PrometheusViewDefinition.MetricType;

/** Set of rules to check if the given request can be served by prometheus */
class QueryRequestEligibilityValidator {

  private final PrometheusViewDefinition prometheusViewDefinition;

  public QueryRequestEligibilityValidator(PrometheusViewDefinition prometheusViewDefinition) {
    this.prometheusViewDefinition = prometheusViewDefinition;
  }

  QueryCost calculateCost(QueryRequest queryRequest, ExecutionContext executionContext) {
    try {
      // orderBy to be supported later
      if (queryRequest.getOrderByCount() > 0) {
        return QueryCost.UNSUPPORTED;
      }

      // only aggregation queries are supported
      if (queryRequest.getAggregationCount() == 0 || queryRequest.getGroupByCount() == 0) {
        return QueryCost.UNSUPPORTED;
      }

      if (queryRequest.getDistinctSelections()) {
        return QueryCost.UNSUPPORTED;
      }

      // all selection including group by and aggregations should be either on column or attribute
      if (executionContext.getAllSelections().stream()
          .filter(Predicate.not(QueryRequestUtil::isDateTimeFunction))
          .anyMatch(Predicate.not(QueryRequestUtil::isSimpleColumnExpression))) {
        return QueryCost.UNSUPPORTED;
      }

      Set<String> referencedColumns = executionContext.getReferencedColumns();
      Preconditions.checkArgument(!referencedColumns.isEmpty());
      // all the columns in the request should have a mapping in the config
      for (String referencedColumn : referencedColumns) {
        if (prometheusViewDefinition.getPhysicalColumnNameForLogicalColumnName(referencedColumn)
                == null
            && prometheusViewDefinition.getMetricConfigForLogicalMetricName(referencedColumn)
                == null) {
          return QueryCost.UNSUPPORTED;
        }
      }

      if (areAggregationsNotSupported(queryRequest.getAggregationList())) {
        return QueryCost.UNSUPPORTED;
      }

      // if selection and groupBy should be on same column or simple attribute
      if (selectionAndGroupByOnDifferentColumn(
          queryRequest.getSelectionList(), queryRequest.getGroupByList())) {
        return QueryCost.UNSUPPORTED;
      }

      if (isFilterNotSupported(queryRequest.getFilter())) {
        return QueryCost.UNSUPPORTED;
      }
    } catch (Exception e) {
      return QueryCost.UNSUPPORTED;
    }
    // value 1.0 so that prometheus is preferred over others
    return new QueryCost(1.0);
  }

  private boolean selectionAndGroupByOnDifferentColumn(
      List<Expression> selectionList, List<Expression> groupByList) {

    Set<String> selections =
        selectionList.stream()
            .map(QueryRequestUtil::getLogicalColumnNameForSimpleColumnExpression)
            .collect(Collectors.toSet());

    Set<String> groupBys =
        groupByList.stream()
            .filter(Predicate.not(QueryRequestUtil::isDateTimeFunction))
            .map(QueryRequestUtil::getLogicalColumnNameForSimpleColumnExpression)
            .collect(Collectors.toSet());
    return !selections.equals(groupBys);
  }

  private boolean areAggregationsNotSupported(List<Expression> aggregationList) {
    // supported aggregation must have single argument (except for dateTimeConvert)
    // prometheusViewDef must have mapping for the metric
    // function type must be supported
    // right now only GAUGE type of metric is supported
    return aggregationList.stream()
        .filter(Predicate.not(QueryRequestUtil::isDateTimeFunction))
        .anyMatch(
            expression -> {
              Function function = expression.getFunction();
              if (function.getArgumentsCount() > 1) {
                return true;
              }
              Expression functionArgument = function.getArgumentsList().get(0);
              String attributeId = getLogicalColumnNameForSimpleColumnExpression(functionArgument);
              if (!PrometheusFunctionConverter.supportedFunctions.contains(
                  function.getFunctionName())) {
                return true;
              }
              MetricConfig metricConfig =
                  prometheusViewDefinition.getMetricConfigForLogicalMetricName(attributeId);
              return null == metricConfig || metricConfig.getMetricType() != MetricType.GAUGE;
            });
  }

  private boolean isFilterNotSupported(Filter filter) {
    if (filter.getChildFilterCount() > 0) {
      // Currently, `AND` high level composite operator is supported
      // later OR operator can be supported for same column
      if (filter.getOperator() != Operator.AND) {
        return true;
      }
      for (Filter childFilter : filter.getChildFilterList()) {
        if (!isFilterNotSupported(childFilter)) {
          return true;
        }
      }
    } else {
      // rhs condition of filter should be literal only
      if (filter.getRhs().getValueCase() != ValueCase.LITERAL) {
        return true;
      }

      // lhs condition of filter should be column or simple attribute
      if (!QueryRequestUtil.isSimpleColumnExpression(filter.getLhs())) {
        return true;
      }
    }

    return false;
  }
}
