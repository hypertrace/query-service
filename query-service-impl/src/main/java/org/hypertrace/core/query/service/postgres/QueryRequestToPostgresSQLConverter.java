package org.hypertrace.core.query.service.postgres;

import java.util.AbstractMap.SimpleEntry;
import java.util.LinkedHashSet;
import java.util.Map.Entry;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.OrderByExpression;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.SortOrder;
import org.hypertrace.core.query.service.postgres.Params.Builder;
import org.hypertrace.core.query.service.postgres.converters.ColumnRequestConverter;
import org.hypertrace.core.query.service.postgres.converters.ColumnRequestConverterFactory;
import org.hypertrace.core.query.service.postgres.converters.PostgresFunctionConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Converts {@link QueryRequest} to Postgres SQL query */
class QueryRequestToPostgresSQLConverter {

  private static final Logger LOG =
      LoggerFactory.getLogger(QueryRequestToPostgresSQLConverter.class);

  private final TableDefinition tableDefinition;
  private final PostgresFunctionConverter functionConverter;

  QueryRequestToPostgresSQLConverter(
      TableDefinition tableDefinition, PostgresFunctionConverter functionConverter) {
    this.tableDefinition = tableDefinition;
    this.functionConverter = functionConverter;
  }

  Entry<String, Params> toSQL(
      ExecutionContext executionContext,
      QueryRequest request,
      LinkedHashSet<Expression> allSelections) {
    ColumnRequestConverter columnRequestConverter =
        ColumnRequestConverterFactory.getColumnRequestConverter(tableDefinition, functionConverter);
    Builder paramsBuilder = Params.newBuilder();
    StringBuilder sqlBuilder = new StringBuilder("Select ");
    String delim = "";

    // Set the DISTINCT keyword if the request has set distinctSelections.
    if (request.getDistinctSelections()) {
      sqlBuilder.append("DISTINCT ");
    }

    // allSelections contain all the various expressions in QueryRequest that we want selections on.
    // Group bys, selections and aggregations in that order. See RequestAnalyzer#analyze() to see
    // how it is created.
    for (Expression expr : allSelections) {
      sqlBuilder.append(delim);
      sqlBuilder.append(
          columnRequestConverter.convertSelectClause(expr, paramsBuilder, executionContext));
      delim = ", ";
    }

    sqlBuilder.append(" FROM public.\"").append(tableDefinition.getTableName()).append("\"");

    // Add the tenantId filter
    sqlBuilder.append(" WHERE ").append(tableDefinition.getTenantIdColumn()).append(" = ?");
    paramsBuilder.addStringParam(executionContext.getTenantId());

    if (request.hasFilter()) {
      sqlBuilder.append(" AND ");
      String filterClause =
          columnRequestConverter.convertFilterClause(
              request.getFilter(), paramsBuilder, executionContext);
      sqlBuilder.append(filterClause);
    }

    if (request.getGroupByCount() > 0) {
      sqlBuilder.append(" GROUP BY ");
      delim = "";
      for (Expression groupByExpression : request.getGroupByList()) {
        sqlBuilder.append(delim);
        sqlBuilder.append(
            columnRequestConverter.convertGroupByClause(
                groupByExpression, paramsBuilder, executionContext));
        delim = ", ";
      }
    }

    if (!request.getOrderByList().isEmpty()) {
      sqlBuilder.append(" ORDER BY ");
      delim = "";
      for (OrderByExpression orderByExpression : request.getOrderByList()) {
        sqlBuilder.append(delim);
        sqlBuilder.append(
            columnRequestConverter.convertOrderByClause(
                orderByExpression.getExpression(), paramsBuilder, executionContext));
        if (SortOrder.DESC.equals(orderByExpression.getOrder())) {
          sqlBuilder.append(" desc ");
        }
        delim = ", ";
      }
    }
    if (request.getLimit() > 0) {
      if (request.getOffset() > 0) {
        sqlBuilder
            .append(" offset ")
            .append(request.getOffset())
            .append(" limit ")
            .append(request.getLimit());
      } else {
        sqlBuilder.append(" limit ").append(request.getLimit());
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Converted QueryRequest to Postgres SQL: {}", sqlBuilder);
    }
    return new SimpleEntry<>(sqlBuilder.toString(), paramsBuilder.build());
  }

  String resolveStatement(String query, Params params) {
    if (query.isEmpty()) {
      return query;
    }
    String[] queryParts = query.split("\\?");

    String[] parameters = new String[queryParts.length];
    params.getStringParams().forEach((i, p) -> parameters[i] = getStringParam(p));
    params.getIntegerParams().forEach((i, p) -> parameters[i] = String.valueOf(p));
    params.getLongParams().forEach((i, p) -> parameters[i] = String.valueOf(p));
    params.getDoubleParams().forEach((i, p) -> parameters[i] = String.valueOf(p));
    params.getFloatParams().forEach((i, p) -> parameters[i] = String.valueOf(p));

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < queryParts.length; i++) {
      sb.append(queryParts[i]);
      sb.append(parameters[i] != null ? parameters[i] : "");
    }
    String statement = sb.toString();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Resolved SQL statement: [{}]", statement);
    }
    return statement;
  }

  String getStringParam(String value) {
    return "'" + value.replace("'", "''") + "'";
  }
}
