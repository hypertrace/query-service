package org.hypertrace.core.query.service.attribubteexpression;

import static org.hypertrace.core.query.service.QueryRequestUtil.createContainsKeyFilter;
import static org.hypertrace.core.query.service.QueryRequestUtil.isAttributeExpressionWithSubpath;

import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.query.service.AbstractQueryTransformation;
import org.hypertrace.core.query.service.QueryRequestUtil;
import org.hypertrace.core.query.service.api.AttributeExpression;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.OrderByExpression;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.slf4j.Logger;

@Slf4j
final class AttributeExpressionSubpathExistsFilteringTransformation
    extends AbstractQueryTransformation {

  @Override
  protected Logger getLogger() {
    return log;
  }

  @Override
  protected QueryRequest rebuildRequest(
      QueryRequest original,
      List<Expression> selections,
      List<Expression> aggregations,
      Filter originalFilter,
      List<Expression> groupBys,
      List<OrderByExpression> orderBys) {
    Filter updatedFilter =
        rebuildFilterForComplexAttributeExpression(originalFilter, orderBys, selections)
            .orElse(originalFilter);

    return super.rebuildRequest(
        original, selections, aggregations, updatedFilter, groupBys, orderBys);
  }

  /*
   * We need the CONTAINS_KEY filter in all filters and order bys dealing with complex
   * attribute expressions as Pinot gives error if particular key is absent. Rest all work fine.
   * To handle order bys, we add the corresponding filter at the top and 'AND' it with the main filter.
   * To handle filter, we modify each filter (say filter1) as : "CONTAINS_KEY AND filter1".
   */
  private Optional<Filter> rebuildFilterForComplexAttributeExpression(
      Filter originalFilter, List<OrderByExpression> orderBys, List<Expression> selections) {

    Set<Filter> rootFilters =
        ImmutableSet.<Filter>builder()
            .add(updateFilterForComplexAttributeExpressionFromFilter(originalFilter))
            .addAll(createFilterForComplexAttributeExpressionFromOrderBy(orderBys))
            .addAll(createFilterForComplexAttributeExpressionFromSelection(selections))
            .build()
            .stream()
            .filter(Predicate.not(Filter.getDefaultInstance()::equals))
            .collect(ImmutableSet.toImmutableSet());

    if (rootFilters.isEmpty()) {
      return Optional.empty();
    }

    if (rootFilters.size() == 1) {
      return rootFilters.stream().findFirst();
    }
    return Optional.of(
        Filter.newBuilder().setOperator(Operator.AND).addAllChildFilter(rootFilters).build());
  }

  private Filter updateFilterForComplexAttributeExpressionFromFilter(Filter originalFilter) {
    /*
     * If childFilter is present, then the expected operators comprise the logical operators.
     * If childFilter is absent, then the filter is a leaf filter which will have lhs and rhs.
     */
    if (originalFilter.getChildFilterCount() > 0) {
      Filter.Builder builder = Filter.newBuilder();
      builder.setOperator(originalFilter.getOperator());
      originalFilter
          .getChildFilterList()
          .forEach(
              childFilter ->
                  builder.addChildFilter(
                      updateFilterForComplexAttributeExpressionFromFilter(childFilter)));
      return builder.build();
    }
    if (isAttributeExpressionWithSubpath(originalFilter.getLhs())) {
      Filter childFilter =
          createContainsKeyFilter(originalFilter.getLhs().getAttributeExpression());
      return Filter.newBuilder()
          .setOperator(Operator.AND)
          .addChildFilter(originalFilter)
          .addChildFilter(childFilter)
          .build();
    }
    return originalFilter;
  }

  private List<Filter> createFilterForComplexAttributeExpressionFromOrderBy(
      List<OrderByExpression> orderByExpressionList) {
    return orderByExpressionList.stream()
        .map(OrderByExpression::getExpression)
        .filter(QueryRequestUtil::isAttributeExpressionWithSubpath)
        .map(Expression::getAttributeExpression)
        .map(QueryRequestUtil::createContainsKeyFilter)
        .collect(Collectors.toList());
  }

  private List<Filter> createFilterForComplexAttributeExpressionFromSelection(
      List<Expression> expressions) {
    return expressions.stream()
        .flatMap(this::getAnyAttributeExpression)
        .map(QueryRequestUtil::createContainsKeyFilter)
        .collect(Collectors.toList());
  }

  private Stream<AttributeExpression> getAnyAttributeExpression(Expression expression) {
    if (expression.hasFunction()) {
      return expression.getFunction().getArgumentsList().stream()
          .flatMap(this::getAnyAttributeExpression);
    }
    return Stream.of(expression)
        .filter(QueryRequestUtil::isAttributeExpressionWithSubpath)
        .map(Expression::getAttributeExpression);
  }
}
