package org.hypertrace.core.query.service.postgres;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
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
        postgresExecutionContext.addResolvedOrderByColumnQuery(
            new SimpleEntry<>(orderByClause, SortOrder.DESC.equals(orderByExpression.getOrder())));
      }
      postgresExecutionContext.addAllOrderByTableColumnNames(
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

    List<String> selectColumns = postgresExecutionContext.getResolvedSelectColumns();
    for (int i = 0; i < selectColumns.size(); i++) {
      String selectColumn = selectColumns.get(i);
      if (!selectColumn.contains("?")) {
        selectedColumnIndexMap.put(selectColumn, "" + (i + 1));
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
    sqlBuilder.append(String.join(", ", postgresExecutionContext.getResolvedSelectColumns()));
    buildFromAndWhereClause(postgresExecutionContext, sqlBuilder);
  }

  private void buildUnnestSelectAndFromAndWhereClause(
      PostgresExecutionContext postgresExecutionContext, StringBuilder sqlBuilder) {
    List<String> selectColumns = postgresExecutionContext.getResolvedSelectColumns();
    List<String> actualSelectColumns = postgresExecutionContext.getSelectTableColumnNames();
    List<String> unnestColumnNames = postgresExecutionContext.getUnnestTableColumnNames();
    if (selectColumns.size() != actualSelectColumns.size()) {
      throw new UnsupportedOperationException(
          "Unable to handle query where column queries and column names are of different sizes");
    }

    Map<String, String> unnestColumnNameMap = new HashMap<>();
    IntStream.range(0, selectColumns.size())
        .boxed()
        .forEach(
            i -> {
              if (i > 0) {
                sqlBuilder.append(", ");
              }
              String actualColumnName = actualSelectColumns.get(i);
              if (unnestColumnNames.contains(actualColumnName)) {
                String columnName =
                    unnestColumnNameMap.computeIfAbsent(
                        actualColumnName, key -> "column" + (i + 1));
                sqlBuilder.append(
                    selectColumns.get(i).replace(actualSelectColumns.get(i), columnName));
              } else {
                sqlBuilder.append(selectColumns.get(i));
              }
            });

    sqlBuilder.append(" FROM ( SELECT ");
    List<String> distinctActualSelectColumns =
        actualSelectColumns.stream().distinct().collect(Collectors.toList());

    sqlBuilder.append(
        distinctActualSelectColumns.stream()
            .map(
                actualColumnName -> {
                  if (unnestColumnNames.contains(actualColumnName)) {
                    return "UNNEST("
                        + actualColumnName
                        + ") AS "
                        + unnestColumnNameMap.get(actualColumnName);
                  } else {
                    return actualColumnName;
                  }
                })
            .collect(Collectors.joining(", ")));

    buildFromAndWhereClause(postgresExecutionContext, sqlBuilder);

    sqlBuilder.append(" ) AS INTERMEDIATE_TABLE");
  }

  private void buildFromAndWhereClause(
      PostgresExecutionContext postgresExecutionContext, StringBuilder sqlBuilder) {
    sqlBuilder.append(" FROM public.\"").append(tableDefinition.getTableName()).append("\"");

    // Add the tenantId filter
    sqlBuilder.append(" WHERE ").append(tableDefinition.getTenantIdColumn()).append(" = ?");

    List<String> filterColumns = postgresExecutionContext.getResolvedFilterColumns();
    if (!filterColumns.isEmpty()) {
      sqlBuilder.append(" AND ");
      sqlBuilder.append(String.join(" AND ", filterColumns));
    }
  }

  private void buildGroupByAndOrderByAndOffsetAndLimitClause(
      QueryRequest request,
      PostgresExecutionContext postgresExecutionContext,
      Map<String, String> selectedColumnIndexMap,
      StringBuilder sqlBuilder) {

    List<String> groupByColumns = postgresExecutionContext.getResolvedGroupByColumns();
    if (!groupByColumns.isEmpty()) {
      sqlBuilder.append(" GROUP BY ");
      sqlBuilder.append(
          groupByColumns.stream()
              .map(
                  groupBy ->
                      Optional.ofNullable(selectedColumnIndexMap.get(groupBy)).orElse(groupBy))
              .collect(Collectors.joining(", ")));
    }

    List<Entry<String, Boolean>> orderByColumns =
        postgresExecutionContext.getResolvedOrderByColumns();
    if (!orderByColumns.isEmpty()) {
      sqlBuilder.append(" ORDER BY ");
      sqlBuilder.append(
          orderByColumns.stream()
              .map(
                  orderByEntry -> {
                    String orderBy =
                        Optional.ofNullable(selectedColumnIndexMap.get(orderByEntry.getKey()))
                            .orElse(orderByEntry.getKey());
                    return orderBy + (Boolean.TRUE.equals(orderByEntry.getValue()) ? " DESC" : "");
                  })
              .collect(Collectors.joining(", ")));
    }

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
