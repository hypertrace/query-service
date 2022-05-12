package org.hypertrace.core.query.service.multivalue;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import java.util.List;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.query.service.AbstractQueryTransformation;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Function;
import org.slf4j.Logger;

@Slf4j
public class MultiValueTransformation extends AbstractQueryTransformation {
  private final FunctionTransformation functionTransformation;

  @Inject
  MultiValueTransformation(FunctionTransformation functionTransformation) {
    this.functionTransformation = functionTransformation;
  }

  @Override
  protected Logger getLogger() {
    return log;
  }

  @Override
  protected Single<Expression> transformFunction(Function function) {
    return this.transformExpressionList(function.getArgumentsList())
        .map(function.toBuilder().clearArguments()::addAllArguments)
        .flatMap(functionTransformation::transformFunction)
        .map(Expression.newBuilder()::setFunction)
        .map(Expression.Builder::build);
  }

  private Single<List<Expression>> transformExpressionList(List<Expression> expressionList) {
    return Observable.fromIterable(expressionList)
        .concatMapSingle(this::transformExpression)
        .toList();
  }
}
