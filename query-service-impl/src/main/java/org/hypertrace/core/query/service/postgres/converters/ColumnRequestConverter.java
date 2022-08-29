package org.hypertrace.core.query.service.postgres.converters;

import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.postgres.Params.Builder;

public interface ColumnRequestConverter {

  String convertSelectClause(
      Expression expression,
      Builder paramsBuilder,
      PostgresExecutionContext postgresExecutionContext);

  String convertFilterClause(
      Filter filter, Builder paramsBuilder, PostgresExecutionContext postgresExecutionContext);

  String convertGroupByClause(
      Expression expression,
      Builder paramsBuilder,
      PostgresExecutionContext postgresExecutionContext);

  String convertOrderByClause(
      Expression expression,
      Builder paramsBuilder,
      PostgresExecutionContext postgresExecutionContext);
}
