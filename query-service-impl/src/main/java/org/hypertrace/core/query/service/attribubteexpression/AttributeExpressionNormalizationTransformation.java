package org.hypertrace.core.query.service.attribubteexpression;

import io.reactivex.rxjava3.core.Single;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.query.service.AbstractQueryTransformation;
import org.hypertrace.core.query.service.QueryRequestUtil;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.Expression;
import org.slf4j.Logger;

@Slf4j
final class AttributeExpressionNormalizationTransformation extends AbstractQueryTransformation {

  @Override
  public int getPriority() {
    // Run before default transformations
    return 1;
  }

  @Override
  protected Logger getLogger() {
    return log;
  }

  @Override
  protected Single<Expression> transformColumnIdentifier(ColumnIdentifier columnIdentifier) {
    Expression.Builder expressionBuilder =
        QueryRequestUtil.createSimpleAttributeExpression(columnIdentifier.getColumnName());
    if (columnIdentifier.getAlias().isBlank()) {
      return Single.just(expressionBuilder.build());
    }

    return Single.just(
        expressionBuilder
            .setAttributeExpression(
                expressionBuilder
                    .getAttributeExpressionBuilder()
                    .setAlias(columnIdentifier.getAlias()))
            .build());
  }
}
