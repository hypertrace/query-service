package org.hypertrace.core.query.service;

import static io.reactivex.rxjava3.core.Single.zip;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import java.util.List;
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

    return original.toBuilder()
        .clearSelection()
        .addAllSelection(selections)
        .clearAggregation()
        .addAllAggregation(aggregations)
        .clearGroupBy()
        .addAllGroupBy(groupBys)
        .clearOrderBy()
        .addAllOrderBy(orderBys)
        .setFilter(filter)
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
    if (filter.equals(Filter.getDefaultInstance())) {
      return Single.just(filter);
    }

    Single<Expression> lhsSingle = this.transformExpression(filter.getLhs());
    Single<Expression> rhsSingle = this.transformExpression(filter.getRhs());
    Single<List<Filter>> childFilterListSingle =
        Observable.fromIterable(filter.getChildFilterList())
            .concatMapSingle(this::transformFilter)
            .toList();
    return zip(
        lhsSingle,
        rhsSingle,
        childFilterListSingle,
        (lhs, rhs, childFilterList) ->
            this.rebuildFilterOmittingDefaults(filter, lhs, rhs, childFilterList));
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
  private Filter rebuildFilterOmittingDefaults(
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

    return builder.clearChildFilter().addAllChildFilter(childFilters).build();
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
