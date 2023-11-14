package org.hypertrace.core.query.service;

import static io.reactivex.rxjava3.core.Single.zip;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.query.service.api.AttributeExpression;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.OrderByExpression;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.slf4j.Logger;

public abstract class AbstractQueryTransformation implements QueryTransformation {

  protected abstract Logger getLogger();

  @Override
  public Single<QueryRequest> transform(
      QueryRequest queryRequest, QueryTransformationContext transformationContext) {
    return zip(
            this.transformExpressionList(queryRequest.getSelectionList()),
            this.transformExpressionList(queryRequest.getAggregationList()),
            this.transformFilter(queryRequest.getFilter()),
            this.transformExpressionList(queryRequest.getGroupByList()),
            this.transformOrderByList(queryRequest.getOrderByList()),
            (selections, aggregations, filter, groupBys, orderBys) ->
                this.rebuildRequest(
                    queryRequest, selections, aggregations, filter, groupBys, orderBys))
        .doOnSuccess(transformed -> this.debugLogIfRequestTransformed(queryRequest, transformed));
  }

  protected QueryRequest rebuildRequest(
      QueryRequest original,
      List<Expression> selections,
      List<Expression> aggregations,
      Filter filter,
      List<Expression> groupBys,
      List<OrderByExpression> orderBys) {

    QueryRequest.Builder builder = original.toBuilder();

    if (Filter.getDefaultInstance().equals(filter)) {
      builder.clearFilter();
    } else {
      builder.setFilter(filter);
    }

    return builder
        .clearSelection()
        .addAllSelection(selections)
        .clearAggregation()
        .addAllAggregation(aggregations)
        .clearGroupBy()
        .addAllGroupBy(groupBys)
        .clearOrderBy()
        .addAllOrderBy(orderBys)
        .build();
  }

  protected Single<Expression> transformExpression(Expression expression) {
    switch (expression.getValueCase()) {
      case COLUMNIDENTIFIER:
        return this.transformColumnIdentifier(expression.getColumnIdentifier());
      case ATTRIBUTE_EXPRESSION:
        return this.transformAttributeExpression(expression.getAttributeExpression());
      case FUNCTION:
        return this.transformFunction(expression.getFunction());
      case ORDERBY:
        return this.transformOrderBy(expression.getOrderBy())
            .map(expression.toBuilder()::setOrderBy)
            .map(Expression.Builder::build);
      case LITERAL:
        return this.transformLiteral(expression.getLiteral());
      case VALUE_NOT_SET:
      default:
        return Single.just(expression);
    }
  }

  protected Single<Expression> transformColumnIdentifier(ColumnIdentifier columnIdentifier) {
    return Single.just(Expression.newBuilder().setColumnIdentifier(columnIdentifier).build());
  }

  protected Single<Expression> transformAttributeExpression(
      AttributeExpression attributeExpression) {
    return Single.just(Expression.newBuilder().setAttributeExpression(attributeExpression).build());
  }

  protected Single<Expression> transformLiteral(LiteralConstant literalConstant) {
    return Single.just(Expression.newBuilder().setLiteral(literalConstant).build());
  }

  protected Single<Expression> transformFunction(Function function) {
    return this.transformExpressionList(function.getArgumentsList())
        .map(expressions -> function.toBuilder().clearArguments().addAllArguments(expressions))
        .map(Function.Builder::build)
        .map(Expression.newBuilder()::setFunction)
        .map(Expression.Builder::build);
  }

  protected Single<OrderByExpression> transformOrderBy(OrderByExpression orderBy) {
    return this.transformExpression(orderBy.getExpression())
        .map(orderBy.toBuilder()::setExpression)
        .map(OrderByExpression.Builder::build);
  }

  protected Single<Filter> transformFilter(Filter filter) {
    return transformFilterInternal(filter)
        .map(filterOptional -> filterOptional.orElseGet(Filter::getDefaultInstance));
  }

  private Single<Optional<Filter>> transformFilterInternal(Filter filter) {
    if (filter.equals(Filter.getDefaultInstance())) {
      return Single.just(Optional.empty());
    }

    Single<Expression> lhsSingle = this.transformExpression(filter.getLhs());
    Single<Expression> rhsSingle = this.transformExpression(filter.getRhs());
    Single<List<Filter>> childFilterListSingle =
        Observable.fromIterable(filter.getChildFilterList())
            .concatMapSingle(this::transformFilterInternal)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .toList();
    return zip(
        lhsSingle,
        rhsSingle,
        childFilterListSingle,
        (lhs, rhs, childFilterList) ->
            this.rebuildFilterOmittingDefaultAndInvalidFilter(filter, lhs, rhs, childFilterList));
  }

  private Single<List<Expression>> transformExpressionList(List<Expression> expressionList) {
    return Observable.fromIterable(expressionList)
        .concatMapSingle(this::transformExpression)
        .toList();
  }

  private Single<List<OrderByExpression>> transformOrderByList(
      List<OrderByExpression> orderByList) {
    return Observable.fromIterable(orderByList).concatMapSingle(this::transformOrderBy).toList();
  }

  /**
   * This doesn't change any functional behavior, but omits fields that aren't needed, shrinking the
   * object and keeping it equivalent to the source object for equality checks.
   */
  private Optional<Filter> rebuildFilterOmittingDefaultAndInvalidFilter(
      Filter original, Expression lhs, Expression rhs, List<Filter> childFilters) {
    Filter.Builder builder = original.toBuilder();

    if (Expression.getDefaultInstance().equals(lhs)) {
      builder.clearLhs();
    } else {
      builder.setLhs(lhs);
    }

    if (Expression.getDefaultInstance().equals(rhs)) {
      builder.clearRhs();
    } else {
      builder.setRhs(rhs);
    }

    Filter transformedFilter = builder.clearChildFilter().addAllChildFilter(childFilters).build();
    // filter should either have child filters or have a valid lhs and rhs
    // only valid filters are considered and other invalid filters are ignored
    if (!transformedFilter.getChildFilterList().isEmpty()
        || (transformedFilter.hasLhs() && transformedFilter.hasRhs())) {
      return Optional.of(transformedFilter);
    } else {
      return Optional.empty();
    }
  }

  private void debugLogIfRequestTransformed(QueryRequest original, QueryRequest transformed) {
    if (!original.equals(transformed)) {
      getLogger()
          .debug(
              "Request transformation occurred. Original request: {} Transformed Request: {}",
              original,
              transformed);
    }
  }
}
