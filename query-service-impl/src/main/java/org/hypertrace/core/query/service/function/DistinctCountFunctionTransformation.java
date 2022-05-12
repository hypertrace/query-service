package org.hypertrace.core.query.service.function;

import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_BOOL_ARRAY;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_DOUBLE_ARRAY;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_INT64_ARRAY;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_STRING_ARRAY;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import java.util.List;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.query.service.AbstractQueryTransformation;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Function;
import org.slf4j.Logger;

@Slf4j
public class DistinctCountFunctionTransformation extends AbstractQueryTransformation {
  protected static final String DISTINCT_COUNT = "DISTINCTCOUNT";
  protected static final String DISTINCT_COUNT_MV = "DISTINCTCOUNTMV";

  private final CachingAttributeClient attributeClient;

  @Inject
  DistinctCountFunctionTransformation(CachingAttributeClient attributeClient) {
    this.attributeClient = attributeClient;
  }

  @Override
  protected Logger getLogger() {
    return log;
  }

  @Override
  protected Single<Expression> transformFunction(Function function) {
    return this.transformExpressionList(function.getArgumentsList())
        .map(function.toBuilder().clearArguments()::addAllArguments)
        .flatMap(
            builder ->
                isTransformable(builder) ? transformDistinctCount(builder) : Single.just(builder))
        .map(Expression.newBuilder()::setFunction)
        .map(Expression.Builder::build);
  }

  private Single<List<Expression>> transformExpressionList(List<Expression> expressionList) {
    return Observable.fromIterable(expressionList)
        .concatMapSingle(this::transformExpression)
        .toList();
  }

  // Checking if the function is of the form DISTINCT_COUNT(ATTRIBUTE_EXPRESSION)
  private boolean isTransformable(Function.Builder builder) {
    return builder.getFunctionName().equals(DISTINCT_COUNT)
        && builder.getArgumentsList().size() == 1
        && builder.getArgumentsList().get(0).hasAttributeExpression();
  }

  private Single<Function.Builder> transformDistinctCount(Function.Builder functionBuilder) {
    return this.getFirstAttributeId(functionBuilder)
        .flatMap(this.attributeClient::get)
        .map(
            metadata ->
                isArray(metadata.getValueKind())
                    ? functionBuilder.setFunctionName(DISTINCT_COUNT_MV)
                    : functionBuilder);
  }

  private boolean isArray(AttributeKind attributeKind) {
    return List.of(TYPE_STRING_ARRAY, TYPE_BOOL_ARRAY, TYPE_DOUBLE_ARRAY, TYPE_INT64_ARRAY)
        .contains(attributeKind);
  }

  private Single<String> getFirstAttributeId(Function.Builder functionBuilder) {
    return Single.just(functionBuilder.getArguments(0).getAttributeExpression().getAttributeId());
  }
}
