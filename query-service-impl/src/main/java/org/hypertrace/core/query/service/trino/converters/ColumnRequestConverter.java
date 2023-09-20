package org.hypertrace.core.query.service.trino.converters;

import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.trino.Params.Builder;

public interface ColumnRequestConverter {

  String convertSelectClause(
      Expression expression, Builder paramsBuilder, TrinoExecutionContext trinoExecutionContext);

  String convertFilterClause(
      Filter filter, Builder paramsBuilder, TrinoExecutionContext trinoExecutionContext);

  String convertGroupByClause(
      Expression expression, Builder paramsBuilder, TrinoExecutionContext trinoExecutionContext);

  String convertOrderByClause(
      Expression expression, Builder paramsBuilder, TrinoExecutionContext trinoExecutionContext);
}
