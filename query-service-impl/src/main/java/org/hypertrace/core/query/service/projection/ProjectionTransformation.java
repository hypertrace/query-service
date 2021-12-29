package org.hypertrace.core.query.service.projection;

import static io.reactivex.rxjava3.core.Single.zip;
import static org.hypertrace.core.query.service.QueryRequestUtil.createBooleanLiteralExpression;
import static org.hypertrace.core.query.service.QueryRequestUtil.createDoubleLiteralExpression;
import static org.hypertrace.core.query.service.QueryRequestUtil.createLongLiteralExpression;
import static org.hypertrace.core.query.service.QueryRequestUtil.createNullNumberLiteralExpression;
import static org.hypertrace.core.query.service.QueryRequestUtil.createNullStringLiteralExpression;
import static org.hypertrace.core.query.service.QueryRequestUtil.createSimpleAttributeExpression;
import static org.hypertrace.core.query.service.QueryRequestUtil.createStringLiteralExpression;

import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.projection.AttributeProjection;
import org.hypertrace.core.attribute.service.projection.AttributeProjectionRegistry;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.attribute.service.v1.Projection;
import org.hypertrace.core.attribute.service.v1.ProjectionExpression;
import org.hypertrace.core.attribute.service.v1.ProjectionOperator;
import org.hypertrace.core.query.service.AbstractQueryTransformation;
import org.hypertrace.core.query.service.QueryFunctionConstants;
import org.hypertrace.core.query.service.QueryRequestUtil;
import org.hypertrace.core.query.service.api.AttributeExpression;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Function;
import org.slf4j.Logger;

@Slf4j
final class ProjectionTransformation extends AbstractQueryTransformation {

  private final CachingAttributeClient attributeClient;
  private final AttributeProjectionRegistry projectionRegistry;

  @Inject
  ProjectionTransformation(
      CachingAttributeClient attributeClient, AttributeProjectionRegistry projectionRegistry) {
    this.attributeClient = attributeClient;
    this.projectionRegistry = projectionRegistry;
  }

  @Override
  protected Logger getLogger() {
    return log;
  }

  @Override
  protected Single<Expression> transformAttributeExpression(
      AttributeExpression attributeExpression) {
    return this.projectAttributeIfPossible(attributeExpression.getAttributeId())
        .map(
            projectedExpression ->
                attributeExpression.hasSubpath()
                    ? this.addSubpathOrThrow(projectedExpression, attributeExpression.getSubpath())
                    : projectedExpression)
        .map(expression -> this.aliasToMatchOriginal(attributeExpression, expression))
        .defaultIfEmpty(
            Expression.newBuilder().setAttributeExpression(attributeExpression).build());
  }

  private Expression addSubpathOrThrow(Expression projectedExpression, String subpath) {
    if (!QueryRequestUtil.isSimpleAttributeExpression(projectedExpression)) {
      throw new IllegalArgumentException(
          "Cannot use subpath for expression with non-trivial projection: " + projectedExpression);
    }
    return Expression.newBuilder()
        .setAttributeExpression(
            projectedExpression.getAttributeExpression().toBuilder().setSubpath(subpath))
        .build();
  }

  private Maybe<Expression> projectAttributeIfPossible(String attributeId) {
    return this.attributeClient
        .get(attributeId)
        .onErrorComplete()
        .flatMap(this::projectAttributeIfPossible);
  }

  private Maybe<Expression> projectAttributeIfPossible(AttributeMetadata attributeMetadata) {
    if (!attributeMetadata.getDefinition().hasProjection()) {
      return Maybe.empty();
    }
    if (attributeMetadata.getMaterialized()) {
      return Maybe.empty();
    }
    return this.rewriteProjectionAsQueryExpression(
            attributeMetadata.getDefinition().getProjection(), attributeMetadata.getValueKind())
        .toMaybe();
  }

  private Single<Expression> rewriteProjectionAsQueryExpression(
      Projection projection, AttributeKind expectedType) {
    switch (projection.getValueCase()) {
      case ATTRIBUTE_ID:
        return this.transformExpression(
            createSimpleAttributeExpression(projection.getAttributeId()).build());
      case LITERAL:
        return this.rewriteLiteralAsQueryExpression(projection.getLiteral(), expectedType);
      case EXPRESSION:
        return this.rewriteProjectionExpressionAsQueryExpression(projection.getExpression());
      case VALUE_NOT_SET:
      default:
        return Single.error(
            new UnsupportedOperationException("Unrecognized projection: " + projection));
    }
  }

  private Single<Expression> rewriteLiteralAsQueryExpression(
      LiteralValue literal, AttributeKind expectedType) {
    switch (literal.getValueCase()) {
      case STRING_VALUE:
        return Single.just(createStringLiteralExpression(literal.getStringValue()));
      case BOOLEAN_VALUE:
        return Single.just(createBooleanLiteralExpression(literal.getBooleanValue()));
      case FLOAT_VALUE:
        return Single.just(createDoubleLiteralExpression(literal.getFloatValue()));
      case INT_VALUE:
        return Single.just(createLongLiteralExpression(literal.getIntValue()));
      case VALUE_NOT_SET:
        return this.getCorrespondingNullExpression(expectedType);
      default:
        return Single.error(
            new UnsupportedOperationException("Unrecognized literal type: " + literal));
    }
  }

  private Single<Expression> getCorrespondingNullExpression(AttributeKind attributeKind) {
    switch (attributeKind) {
      case TYPE_DOUBLE:
      case TYPE_DOUBLE_ARRAY:
      case TYPE_INT64:
      case TYPE_INT64_ARRAY:
      case TYPE_TIMESTAMP:
        return Single.just(createNullNumberLiteralExpression());
      case TYPE_BOOL:
      case TYPE_BOOL_ARRAY:
      case TYPE_STRING:
      case TYPE_STRING_ARRAY:
      case TYPE_STRING_MAP:
      case TYPE_BYTES:
        return Single.just(createNullStringLiteralExpression());
      case UNRECOGNIZED:
      case KIND_UNDEFINED:
      default:
        return Single.error(
            new UnsupportedOperationException(
                "Unrecognized attribute kind for null expression conversion: " + attributeKind));
    }
  }

  private Single<Expression> rewriteProjectionExpressionAsQueryExpression(
      ProjectionExpression projectionExpression) {
    List<Projection> arguments = projectionExpression.getArgumentsList();
    Optional<List<AttributeKind>> knownArgumentKinds =
        this.projectionRegistry
            .getProjection(projectionExpression.getOperator())
            .map(AttributeProjection::getArgumentKinds);

    Single<List<Expression>> argumentListSingle =
        Observable.range(0, arguments.size())
            .concatMapSingle(
                index ->
                    rewriteProjectionAsQueryExpression(
                        arguments.get(index),
                        knownArgumentKinds
                            .map(kinds -> kinds.get(index))
                            .orElse(AttributeKind.KIND_UNDEFINED)))
            .toList();

    Single<String> operatorSingle = this.convertOperator(projectionExpression.getOperator());

    return zip(
        argumentListSingle,
        operatorSingle,
        (argumentExpressions, operatorName) ->
            Expression.newBuilder()
                .setFunction(
                    Function.newBuilder()
                        .setFunctionName(operatorName)
                        .addAllArguments(argumentExpressions))
                .build());
  }

  private Single<String> convertOperator(ProjectionOperator operator) {
    switch (operator) {
      case PROJECTION_OPERATOR_CONCAT:
        return Single.just(QueryFunctionConstants.QUERY_FUNCTION_CONCAT);
      case PROJECTION_OPERATOR_CONCAT_OR_NULL:
        return Single.just(QueryFunctionConstants.QUERY_FUNCTION_CONCAT_OR_NULL);
      case PROJECTION_OPERATOR_HASH:
        return Single.just(QueryFunctionConstants.QUERY_FUNCTION_HASH);
      case PROJECTION_OPERATOR_STRING_EQUALS:
        return Single.just(QueryFunctionConstants.QUERY_FUNCTION_STRINGEQUALS);
      case PROJECTION_OPERATOR_CONDITIONAL:
        return Single.just(QueryFunctionConstants.QUERY_FUNCTION_CONDITIONAL);
      case PROJECTION_OPERATOR_UNSET:
      case UNRECOGNIZED:
      default:
        return Single.error(
            new UnsupportedOperationException("Unrecognized operator: " + operator));
    }
  }

  private Expression aliasToMatchOriginal(AttributeExpression original, Expression newExpression) {
    String originalKey = QueryRequestUtil.getAlias(original);
    switch (newExpression.getValueCase()) {
      case ATTRIBUTE_EXPRESSION:
        return newExpression.toBuilder()
            .setAttributeExpression(
                newExpression.getAttributeExpression().toBuilder().setAlias(originalKey))
            .build();
      case FUNCTION:
        return newExpression.toBuilder()
            .setFunction(newExpression.getFunction().toBuilder().setAlias(originalKey))
            .build();
      case ORDERBY: // Rest of expressions types don't support aliases
      case LITERAL:
      case VALUE_NOT_SET:
      default:
        return newExpression;
    }
  }
}
