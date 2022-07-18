package org.hypertrace.core.query.service.postgres.converters;

import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.postgres.Params.Builder;

public interface ColumnRequestConverter {

  String convertExpressionToString(
      Expression expression,
      Builder paramsBuilder,
      ExecutionContext executionContext,
      ColumnRequestContext context);

  String convertFilterToString(
      Filter filter,
      Builder paramsBuilder,
      ExecutionContext executionContext,
      ColumnRequestContext context);
}
