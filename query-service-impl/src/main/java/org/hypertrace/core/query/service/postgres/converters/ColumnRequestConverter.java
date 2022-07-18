package org.hypertrace.core.query.service.postgres.converters;

import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.postgres.Params.Builder;

public interface ColumnRequestConverter {

  String convertSelectClause(
      Expression expression, Builder paramsBuilder, ExecutionContext executionContext);

  String convertFilterClause(
      Filter filter, Builder paramsBuilder, ExecutionContext executionContext);

  String convertGroupByClause(
      Expression expression, Builder paramsBuilder, ExecutionContext executionContext);

  String convertOrderByClause(
      Expression expression, Builder paramsBuilder, ExecutionContext executionContext);
}
