package org.hypertrace.core.query.service.postgres;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.IntStream;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.OrderByExpression;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.SortOrder;
import org.hypertrace.core.query.service.postgres.Params.Builder;
import org.hypertrace.core.query.service.postgres.converters.ColumnRequestConverter;
import org.hypertrace.core.query.service.postgres.converters.ColumnRequestConverterFactory;
import org.hypertrace.core.query.service.postgres.converters.PostgresExecutionContext;
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
    PostgresExecutionContext postgresExecutionContext =
        new PostgresExecutionContext(executionContext);
    ColumnRequestConverter columnRequestConverter =
        ColumnRequestConverterFactory.getColumnRequestConverter(tableDefinition, functionConverter);

    // allSelections contain all the various expressions in QueryRequest that we want selections on.
    // Group bys, selections and aggregations in that order. See RequestAnalyzer#analyze() to see
    // how it is created.
    Builder paramsBuilder = Params.newBuilder();
    for (Expression expr : allSelections) {
      String selectClause =
          columnRequestConverter.convertSelectClause(expr, paramsBuilder, postgresExecutionContext);
      postgresExecutionContext.addResolvedSelectColumnQuery(selectClause);
    }
    postgresExecutionContext.addAllSelectTableColumnNames(
        postgresExecutionContext.getActualTableColumnNames());
    postgresExecutionContext.clearActualTableColumnNames();

    paramsBuilder.addStringParam(postgresExecutionContext.getExecutionContext().getTenantId());
    if (request.hasFilter()) {
      String filterClause =
          columnRequestConverter.convertFilterClause(
              request.getFilter(), paramsBuilder, postgresExecutionContext);
      postgresExecutionContext.addResolvedFilterColumnQuery(filterClause);
    }
    postgresExecutionContext.addAllFilterTableColumnNames(
        postgresExecutionContext.getActualTableColumnNames());
    postgresExecutionContext.clearActualTableColumnNames();

    if (request.getGroupByCount() > 0) {
      for (Expression groupByExpression : request.getGroupByList()) {
        String groupByClause =
            columnRequestConverter.convertGroupByClause(
                groupByExpression, paramsBuilder, postgresExecutionContext);
        postgresExecutionContext.addResolvedGroupByColumnQuery(groupByClause);
      }
      postgresExecutionContext.addAllGroupByTableColumnNames(
          postgresExecutionContext.getActualTableColumnNames());
      postgresExecutionContext.clearActualTableColumnNames();
    }

    if (!request.getOrderByList().isEmpty()) {
      for (OrderByExpression orderByExpression : request.getOrderByList()) {
        String orderByClause =
            columnRequestConverter.convertOrderByClause(
                orderByExpression.getExpression(), paramsBuilder, postgresExecutionContext);
        postgresExecutionContext.addResolvedOrderByColumnQuery(orderByClause);
        postgresExecutionContext.addResolvedOrderByDescBool(
            SortOrder.DESC.equals(orderByExpression.getOrder()));
      }
      postgresExecutionContext.addAllGroupByTableColumnNames(
          postgresExecutionContext.getActualTableColumnNames());
      postgresExecutionContext.clearActualTableColumnNames();
    }

    return new SimpleEntry<>(
        buildSqlQuery(request, postgresExecutionContext), paramsBuilder.build());
  }

  private String buildSqlQuery(
      QueryRequest request, PostgresExecutionContext postgresExecutionContext) {
    Map<String, String> selectedColumnIndexMap = new HashMap<>();
    StringBuilder sqlBuilder = new StringBuilder("SELECT ");

    // Set the DISTINCT keyword if the request has set distinctSelections.
    if (request.getDistinctSelections()) {
      sqlBuilder.append("DISTINCT ");
    }

    List<String> selectColumnQueries = postgresExecutionContext.getResolvedSelectColumnQueries();
    for (int i = 0; i < selectColumnQueries.size(); i++) {
      String selectedColumnQuery = selectColumnQueries.get(i);
      if (!selectedColumnQuery.contains("?")) {
        selectedColumnIndexMap.put(selectedColumnQuery, "" + (i + 1));
      }
    }

    if (postgresExecutionContext.getUnnestTableColumnNames().isEmpty()) {
      buildSelectAndFromAndWhereClause(postgresExecutionContext, sqlBuilder);
    } else {
      buildUnnestSelectAndFromAndWhereClause(postgresExecutionContext, sqlBuilder);
    }

    buildGroupByAndOrderByAndOffsetAndLimitClause(
        request, postgresExecutionContext, selectedColumnIndexMap, sqlBuilder);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Converted QueryRequest to Postgres SQL: {}", sqlBuilder);
    }

    return sqlBuilder.toString();
  }

  private void buildSelectAndFromAndWhereClause(
      PostgresExecutionContext postgresExecutionContext, StringBuilder sqlBuilder) {
    List<String> selectColumnQueries = postgresExecutionContext.getResolvedSelectColumnQueries();
    IntStream.range(0, selectColumnQueries.size())
        .boxed()
        .forEach(
            i -> {
              if (i > 0) {
                sqlBuilder.append(", ");
              }
              sqlBuilder.append(selectColumnQueries.get(i));
            });

    buildFromAndWhereClause(postgresExecutionContext, sqlBuilder);
  }

  private void buildUnnestSelectAndFromAndWhereClause(
      PostgresExecutionContext postgresExecutionContext, StringBuilder sqlBuilder) {
    List<String> selectColumnQueries = postgresExecutionContext.getResolvedSelectColumnQueries();
    List<String> actualSelectColumns = postgresExecutionContext.getSelectTableColumnNames();
    List<String> unnestColumnNames = postgresExecutionContext.getUnnestTableColumnNames();
    if (selectColumnQueries.size() != actualSelectColumns.size()) {
      throw new UnsupportedOperationException(
          "No able to handle query where column queries and column names are of different sizes");
    }

    IntStream.range(0, selectColumnQueries.size())
        .boxed()
        .forEach(
            i -> {
              if (i > 0) {
                sqlBuilder.append(", ");
              }
              String actualColumnName = actualSelectColumns.get(i);
              if (unnestColumnNames.contains(actualColumnName)) {
                String columnName = "column" + (i + 1);
                sqlBuilder.append(
                    selectColumnQueries.get(i).replace(actualSelectColumns.get(i), columnName));
              } else {
                sqlBuilder.append(selectColumnQueries.get(i));
              }
            });

    sqlBuilder.append(" FROM ( SELECT ");

    IntStream.range(0, actualSelectColumns.size())
        .boxed()
        .forEach(
            i -> {
              if (i > 0) {
                sqlBuilder.append(", ");
              }
              String actualColumnName = actualSelectColumns.get(i);
              if (unnestColumnNames.contains(actualColumnName)) {
                sqlBuilder.append("UNNEST(");
                sqlBuilder.append(actualColumnName);
                sqlBuilder.append(") AS ");
                String columnName = "column" + (i + 1);
                sqlBuilder.append(columnName);
              } else {
                sqlBuilder.append(actualSelectColumns.get(i));
              }
            });

    buildFromAndWhereClause(postgresExecutionContext, sqlBuilder);

    sqlBuilder.append(" ) AS INTERMEDIATE_TABLE");
  }

  private void buildFromAndWhereClause(
      PostgresExecutionContext postgresExecutionContext, StringBuilder sqlBuilder) {
    sqlBuilder.append(" FROM public.\"").append(tableDefinition.getTableName()).append("\"");

    // Add the tenantId filter
    sqlBuilder.append(" WHERE ").append(tableDefinition.getTenantIdColumn()).append(" = ?");

    List<String> filterColumnQueries = postgresExecutionContext.getResolvedFilterColumnQueries();
    IntStream.range(0, filterColumnQueries.size())
        .boxed()
        .forEach(
            i -> {
              if (i == 0) {
                sqlBuilder.append(" ");
              }
              sqlBuilder.append("AND ");
              sqlBuilder.append(filterColumnQueries.get(i));
            });
  }

  private void buildGroupByAndOrderByAndOffsetAndLimitClause(
      QueryRequest request,
      PostgresExecutionContext postgresExecutionContext,
      Map<String, String> selectedColumnIndexMap,
      StringBuilder sqlBuilder) {

    List<String> groupByColumnQueries = postgresExecutionContext.getResolvedGroupByColumnQueries();
    IntStream.range(0, groupByColumnQueries.size())
        .boxed()
        .forEach(
            i -> {
              if (i == 0) {
                sqlBuilder.append(" GROUP BY ");
              } else {
                sqlBuilder.append(", ");
              }
              String groupByQuery = groupByColumnQueries.get(i);
              sqlBuilder.append(
                  Optional.ofNullable(selectedColumnIndexMap.get(groupByQuery))
                      .orElse(groupByQuery));
            });

    List<String> orderByColumnQueries = postgresExecutionContext.getResolvedOrderByColumnQueries();
    List<Boolean> orderByDescBool = postgresExecutionContext.getResolvedOrderByDescBool();
    IntStream.range(0, orderByColumnQueries.size())
        .boxed()
        .forEach(
            i -> {
              if (i == 0) {
                sqlBuilder.append(" ORDER BY ");
              } else {
                sqlBuilder.append(", ");
              }
              String orderByQuery = orderByColumnQueries.get(i);
              sqlBuilder.append(
                  Optional.ofNullable(selectedColumnIndexMap.get(orderByQuery))
                      .orElse(orderByQuery));
              if (orderByDescBool.get(i)) {
                sqlBuilder.append(" DESC");
              }
            });

    if (request.getLimit() > 0) {
      if (request.getOffset() > 0) {
        sqlBuilder
            .append(" OFFSET ")
            .append(request.getOffset())
            .append(" LIMIT ")
            .append(request.getLimit());
      } else {
        sqlBuilder.append(" LIMIT ").append(request.getLimit());
      }
    }
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
