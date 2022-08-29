package org.hypertrace.core.query.service.postgres.converters;

import java.util.ArrayList;
import java.util.List;
import org.hypertrace.core.query.service.ExecutionContext;

public class PostgresExecutionContext {
  private final ExecutionContext executionContext;
  private final List<String> actualTableColumnNames;
  private final List<String> unnestTableColumnNames;
  private final List<String> selectTableColumnNames;
  private final List<String> filterTableColumnNames;
  private final List<String> groupByTableColumnNames;
  private final List<String> orderByTableColumnNames;
  private final List<String> resolvedSelectColumnQueries;
  private final List<String> resolvedFilterColumnQueries;
  private final List<String> resolvedGroupByColumnQueries;
  private final List<String> resolvedOrderByColumnQueries;
  private final List<Boolean> resolvedOrderByDescBool;

  private ColumnRequestContext columnRequestContext;

  public PostgresExecutionContext(ExecutionContext executionContext) {
    this.executionContext = executionContext;
    this.actualTableColumnNames = new ArrayList<>();
    this.unnestTableColumnNames = new ArrayList<>();
    this.selectTableColumnNames = new ArrayList<>();
    this.filterTableColumnNames = new ArrayList<>();
    this.groupByTableColumnNames = new ArrayList<>();
    this.orderByTableColumnNames = new ArrayList<>();
    this.resolvedSelectColumnQueries = new ArrayList<>();
    this.resolvedFilterColumnQueries = new ArrayList<>();
    this.resolvedGroupByColumnQueries = new ArrayList<>();
    this.resolvedOrderByColumnQueries = new ArrayList<>();
    this.resolvedOrderByDescBool = new ArrayList<>();
  }

  public ExecutionContext getExecutionContext() {
    return executionContext;
  }

  public List<String> getActualTableColumnNames() {
    return actualTableColumnNames;
  }

  public ColumnRequestContext getColumnRequestContext() {
    return columnRequestContext;
  }

  public List<String> getUnnestTableColumnNames() {
    return unnestTableColumnNames;
  }

  public List<String> getSelectTableColumnNames() {
    return selectTableColumnNames;
  }

  public List<String> getFilterTableColumnNames() {
    return filterTableColumnNames;
  }

  public List<String> getGroupByTableColumnNames() {
    return groupByTableColumnNames;
  }

  public List<String> getOrderByTableColumnNames() {
    return orderByTableColumnNames;
  }

  public List<String> getResolvedSelectColumnQueries() {
    return resolvedSelectColumnQueries;
  }

  public List<String> getResolvedFilterColumnQueries() {
    return resolvedFilterColumnQueries;
  }

  public List<String> getResolvedGroupByColumnQueries() {
    return resolvedGroupByColumnQueries;
  }

  public List<String> getResolvedOrderByColumnQueries() {
    return resolvedOrderByColumnQueries;
  }

  public List<Boolean> getResolvedOrderByDescBool() {
    return resolvedOrderByDescBool;
  }

  public void addActualTableColumnName(String columnName) {
    actualTableColumnNames.add(columnName);
  }

  public void addUnnestTableColumnName(String columnName) {
    unnestTableColumnNames.add(columnName);
  }

  public void addAllSelectTableColumnNames(List<String> columnNames) {
    selectTableColumnNames.addAll(columnNames);
  }

  public void addAllFilterTableColumnNames(List<String> columnNames) {
    filterTableColumnNames.addAll(columnNames);
  }

  public void addAllGroupByTableColumnNames(List<String> columnNames) {
    groupByTableColumnNames.addAll(columnNames);
  }

  public void addAllOrderByTableColumnNames(List<String> columnNames) {
    orderByTableColumnNames.addAll(columnNames);
  }

  public void addResolvedSelectColumnQuery(String resolvedColumnQuery) {
    resolvedSelectColumnQueries.add(resolvedColumnQuery);
  }

  public void addResolvedFilterColumnQuery(String resolvedColumnQuery) {
    resolvedFilterColumnQueries.add(resolvedColumnQuery);
  }

  public void addResolvedGroupByColumnQuery(String resolvedColumnQuery) {
    resolvedGroupByColumnQueries.add(resolvedColumnQuery);
  }

  public void addResolvedOrderByColumnQuery(String resolvedColumnQuery) {
    resolvedOrderByColumnQueries.add(resolvedColumnQuery);
  }

  public void addResolvedOrderByDescBool(boolean desc) {
    resolvedOrderByDescBool.add(desc);
  }

  public void resetColumnRequestContext() {
    this.columnRequestContext = null;
  }

  public void setColumnRequestContext(ColumnRequestContext columnRequestContext) {
    this.columnRequestContext = columnRequestContext;
  }

  public void clearActualTableColumnNames() {
    actualTableColumnNames.clear();
  }
}
