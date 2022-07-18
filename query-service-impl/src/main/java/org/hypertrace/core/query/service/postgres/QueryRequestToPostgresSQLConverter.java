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
import org.hypertrace.core.query.service.postgres.converters.ColumnRequestContext;
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
    ColumnRequestContext context = new ColumnRequestContext();
    ColumnRequestConverter columnRequestConverter =
        ColumnRequestConverterFactory.getColumnRequestConverter(tableDefinition, functionConverter);
    Builder paramsBuilder = Params.newBuilder();
    StringBuilder pqlBuilder = new StringBuilder("Select ");
    String delim = "";

    // Set the DISTINCT keyword if the request has set distinctSelections.
    if (request.getDistinctSelections()) {
      pqlBuilder.append("DISTINCT ");
    }

    context.setQueryPart(ColumnRequestContext.QueryPart.SELECT);
    // allSelections contain all the various expressions in QueryRequest that we want selections on.
    // Group bys, selections and aggregations in that order. See RequestAnalyzer#analyze() to see
    // how it is created.
    for (Expression expr : allSelections) {
      pqlBuilder.append(delim);
      pqlBuilder.append(
          columnRequestConverter.convertExpressionToString(
              expr, paramsBuilder, executionContext, context));
      delim = ", ";
    }

    pqlBuilder.append(" FROM public.\"").append(tableDefinition.getTableName()).append("\"");

    context.setQueryPart(ColumnRequestContext.QueryPart.WHERE);

    // Add the tenantId filter
    pqlBuilder.append(" WHERE ").append(tableDefinition.getTenantIdColumn()).append(" = ?");
    paramsBuilder.addStringParam(executionContext.getTenantId());

    if (request.hasFilter()) {
      pqlBuilder.append(" AND ");
      String filterClause =
          columnRequestConverter.convertFilterToString(
              request.getFilter(), paramsBuilder, executionContext, context);
      pqlBuilder.append(filterClause);
    }

    if (request.getGroupByCount() > 0) {
      context.setQueryPart(ColumnRequestContext.QueryPart.GROUP_BY);
      pqlBuilder.append(" GROUP BY ");
      delim = "";
      for (Expression groupByExpression : request.getGroupByList()) {
        pqlBuilder.append(delim);
        pqlBuilder.append(
            columnRequestConverter.convertExpressionToString(
                groupByExpression, paramsBuilder, executionContext, context));
        delim = ", ";
      }
    }

    if (!request.getOrderByList().isEmpty()) {
      context.setQueryPart(ColumnRequestContext.QueryPart.ORDER_BY);
      pqlBuilder.append(" ORDER BY ");
      delim = "";
      for (OrderByExpression orderByExpression : request.getOrderByList()) {
        pqlBuilder.append(delim);
        pqlBuilder.append(
            columnRequestConverter.convertExpressionToString(
                orderByExpression.getExpression(), paramsBuilder, executionContext, context));
        if (SortOrder.DESC.equals(orderByExpression.getOrder())) {
          pqlBuilder.append(" desc ");
        }
        delim = ", ";
      }
    }
    if (request.getLimit() > 0) {
      if (request.getOffset() > 0) {
        pqlBuilder
            .append(" limit ")
            .append(request.getOffset())
            .append(", ")
            .append(request.getLimit());
      } else {
        pqlBuilder.append(" limit ").append(request.getLimit());
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Converted QueryRequest to Postgres SQL: {}", pqlBuilder);
    }
    return new SimpleEntry<>(pqlBuilder.toString(), paramsBuilder.build());
  }
}
