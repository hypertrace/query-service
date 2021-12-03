package org.hypertrace.core.query.service.prometheus;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.QueryCost;
import org.hypertrace.core.query.service.QueryRequestUtil;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Expression.ValueCase;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;

/** Set of rules to check if the given request can be served by prometheus */
class QueryRequestEligibilityValidator {

  private final PrometheusViewDefinition prometheusViewDefinition;

  public QueryRequestEligibilityValidator(PrometheusViewDefinition prometheusViewDefinition) {
    this.prometheusViewDefinition = prometheusViewDefinition;
  }

  QueryCost isEligible(QueryRequest queryRequest, ExecutionContext executionContext) {
    // orderBy to be supported later
    if (queryRequest.getOrderByCount() > 0) {
      return QueryCost.UNSUPPORTED;
    }

    // only aggregation queries are supported
    if (queryRequest.getAggregationCount() == 0
        || queryRequest.getGroupByCount() == 0
        || queryRequest.getDistinctSelections()) {
      return QueryCost.UNSUPPORTED;
    }

    Set<String> referencedColumns = executionContext.getReferencedColumns();
    Preconditions.checkArgument(!referencedColumns.isEmpty());
    // all the columns in the request should have a mapping in the config
    for (String referencedColumn : referencedColumns) {
      if (!QueryRequestUtil.isTimeColumn(referencedColumn)
          && prometheusViewDefinition.getPhysicalColumnName(referencedColumn) == null
          && prometheusViewDefinition.getMetricConfig(referencedColumn) == null) {
        return QueryCost.UNSUPPORTED;
      }
    }

    if (!analyseAggregationColumns(queryRequest.getAggregationList())) {
      return QueryCost.UNSUPPORTED;
    }

    if (!selectionAndGroupByOnSameColumn(
        queryRequest.getSelectionList(), queryRequest.getGroupByList())) {
      return QueryCost.UNSUPPORTED;
    }

    if (!analyseFilter(queryRequest.getFilter())) {
      return QueryCost.UNSUPPORTED;
    }

    // value 1.0 so that prometheus is preferred over others
    return new QueryCost(1.0);
  }

  private boolean selectionAndGroupByOnSameColumn(
      List<Expression> selectionList, List<Expression> groupByList) {
    Set<String> selections = Sets.newHashSet();
    for (Expression expression : selectionList) {
      if (!QueryRequestUtil.isSimpleColumnExpression(expression)) {
        return false;
      }
      selections.add(getLogicalColumnName(expression));
    }

    for (Expression expression : groupByList) {
      // skip datetime convert group by
      if (QueryRequestUtil.isDateTimeFunction(expression)) {
        continue;
      }
      if (!QueryRequestUtil.isSimpleColumnExpression(expression)) {
        return false;
      }
      if (!selections.remove(getLogicalColumnName(expression))) {
        return false;
      }
    }
    return selections.isEmpty();
  }

  private String getLogicalColumnName(Expression expression) {
    String logicalColumnName;
    if (expression.getValueCase() == ValueCase.COLUMNIDENTIFIER) {
      logicalColumnName = expression.getColumnIdentifier().getColumnName();
    } else {
      logicalColumnName = expression.getAttributeExpression().getAttributeId();
    }
    return logicalColumnName;
  }

  private boolean analyseAggregationColumns(List<Expression> aggregationList) {
    for (Expression expression : aggregationList) {
      Function function = expression.getFunction();
      // skip dateTimeConvert function as it is part of the prometheus api call
      if (QueryRequestUtil.isDateTimeFunction(expression)) {
        continue;
      }
      if (function.getArgumentsCount() > 1) {
        return false;
      }
      Expression functionExpression = function.getArgumentsList().get(0);
      if (!QueryRequestUtil.isSimpleColumnExpression(functionExpression)) {
        return false;
      }
      String attributeName =
          (functionExpression.getValueCase() == ValueCase.COLUMNIDENTIFIER)
              ? functionExpression.getColumnIdentifier().getColumnName()
              : functionExpression.getAttributeExpression().getAttributeId();
      if (prometheusViewDefinition.getMetricConfig(attributeName) == null) {
        return false;
      }
      // todo check if the function is supported or not
    }

    return true;
  }

  private boolean analyseFilter(Filter filter) {
    if (filter.getChildFilterCount() > 0) {
      if (filter.getOperator() != Operator.AND) {
        return false;
      }
      for (Filter childFilter : filter.getChildFilterList()) {
        if (!analyseFilter(childFilter)) {
          return false;
        }
      }
    }

    // filter rhs should be literal only
    if (filter.getRhs().getValueCase() != ValueCase.LITERAL) {
      return false;
    }

    // filter lhs should be column or simple attribute
    if (!QueryRequestUtil.isSimpleColumnExpression(filter.getLhs())) {
      return false;
    }

    // todo check for valid operators here
    return true;
  }
}
