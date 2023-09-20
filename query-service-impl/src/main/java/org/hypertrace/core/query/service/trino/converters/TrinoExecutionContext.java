package org.hypertrace.core.query.service.trino.converters;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import org.hypertrace.core.query.service.ExecutionContext;

public class TrinoExecutionContext {
  private final ExecutionContext executionContext;
  private final List<String> actualTableColumnNames;
  private final List<String> unnestTableColumnNames;
  private final List<String> selectTableColumnNames;
  private final List<String> filterTableColumnNames;
  private final List<String> groupByTableColumnNames;
  private final List<String> orderByTableColumnNames;
  private final List<String> resolvedSelectColumns;
  private final List<String> resolvedFilterColumns;
  private final List<String> resolvedGroupByColumns;
  private final List<Entry<String, Boolean>> resolvedOrderByColumns;

  private ColumnRequestContext columnRequestContext;

  public TrinoExecutionContext(ExecutionContext executionContext) {
    this.executionContext = executionContext;
    this.actualTableColumnNames = new ArrayList<>();
    this.unnestTableColumnNames = new ArrayList<>();
    this.selectTableColumnNames = new ArrayList<>();
    this.filterTableColumnNames = new ArrayList<>();
    this.groupByTableColumnNames = new ArrayList<>();
    this.orderByTableColumnNames = new ArrayList<>();
    this.resolvedSelectColumns = new ArrayList<>();
    this.resolvedFilterColumns = new ArrayList<>();
    this.resolvedGroupByColumns = new ArrayList<>();
    this.resolvedOrderByColumns = new ArrayList<>();
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

  public List<String> getResolvedSelectColumns() {
    return resolvedSelectColumns;
  }

  public List<String> getResolvedFilterColumns() {
    return resolvedFilterColumns;
  }

  public List<String> getResolvedGroupByColumns() {
    return resolvedGroupByColumns;
  }

  public List<Entry<String, Boolean>> getResolvedOrderByColumns() {
    return resolvedOrderByColumns;
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
    resolvedSelectColumns.add(resolvedColumnQuery);
  }

  public void addResolvedFilterColumnQuery(String resolvedColumnQuery) {
    resolvedFilterColumns.add(resolvedColumnQuery);
  }

  public void addResolvedGroupByColumnQuery(String resolvedColumnQuery) {
    resolvedGroupByColumns.add(resolvedColumnQuery);
  }

  public void addResolvedOrderByColumnQuery(Entry<String, Boolean> resolvedColumnQuery) {
    resolvedOrderByColumns.add(resolvedColumnQuery);
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
