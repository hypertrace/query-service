package org.hypertrace.core.query.service;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Expression.ValueCase;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.OrderByExpression;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetMetadata;
import org.hypertrace.core.query.service.api.ValueType;

/**
 * Wrapper class to hold the query execution context that is needed by different components during
 * the life cycles of a request.
 */
public class ExecutionContext {

  private final String tenantId;
  private Set<String> referencedColumns;
  private final LinkedHashSet<String> selectedColumns;
  private ResultSetMetadata resultSetMetadata;
  // default value null
  private String timeFilterColumn = null;
  private Optional<Duration> timeRangeDuration;
  // Contains all selections to be made in the DB: selections on group by, single columns and
  // aggregations in that order.
  // There should be a one-to-one mapping between this and the columnMetadataSet in
  // ResultSetMetadata.
  // The difference between this and selectedColumns above is that this is a set of Expressions
  // while the selectedColumns
  // is a set of column names.
  private final LinkedHashSet<Expression> allSelections;
  private final Optional<Duration> timeSeriesPeriod;

  public ExecutionContext(String tenantId, QueryRequest request) {
    this.tenantId = tenantId;
    this.selectedColumns = new LinkedHashSet<>();
    this.allSelections = new LinkedHashSet<>();
    this.timeSeriesPeriod = calculateTimeSeriesPeriod(request);
    analyze(request);
  }

  private Optional<Duration> calculateTimeSeriesPeriod(QueryRequest request) {
    if (request.getGroupByCount() > 0) {
      for (Expression expression : request.getGroupByList()) {
        if (expression.getValueCase() == ValueCase.FUNCTION
            && expression.getFunction().getFunctionName().equals("dateTimeConvert")) {
          String timeSeriesPeriod =
              expression
                  .getFunction()
                  .getArgumentsList()
                  .get(3)
                  .getLiteral()
                  .getValue()
                  .getString();
          return Optional.of(parseDuration(timeSeriesPeriod));
        }
      }
    }
    return Optional.empty();
  }

  private void analyze(QueryRequest request) {
    List<String> filterColumns = new ArrayList<>();
    LinkedList<Filter> filterQueue = new LinkedList<>();
    filterQueue.add(request.getFilter());
    while (!filterQueue.isEmpty()) {
      Filter filter = filterQueue.pop();
      if (filter.getChildFilterCount() > 0) {
        filterQueue.addAll(filter.getChildFilterList());
      } else {
        extractColumns(filterColumns, filter.getLhs());
        extractColumns(filterColumns, filter.getRhs());
      }
    }

    List<String> postFilterColumns = new ArrayList<>();
    List<String> selectedList = new ArrayList<>();
    LinkedHashSet<ColumnMetadata> columnMetadataSet = new LinkedHashSet<>();

    // group by columns must be first in the response
    if (request.getGroupByCount() > 0) {
      for (Expression expression : request.getGroupByList()) {
        extractColumns(postFilterColumns, expression);
        columnMetadataSet.add(toColumnMetadata(expression));
        allSelections.add(expression);
      }
    }
    if (request.getSelectionCount() > 0) {
      for (Expression expression : request.getSelectionList()) {
        extractColumns(selectedList, expression);
        postFilterColumns.addAll(selectedList);
        columnMetadataSet.add(toColumnMetadata(expression));
        allSelections.add(expression);
      }
    }
    if (request.getAggregationCount() > 0) {
      for (Expression expression : request.getAggregationList()) {
        extractColumns(postFilterColumns, expression);
        columnMetadataSet.add(toColumnMetadata(expression));
        allSelections.add(expression);
      }
    }

    referencedColumns = new HashSet<>();
    referencedColumns.addAll(filterColumns);
    referencedColumns.addAll(postFilterColumns);
    resultSetMetadata =
        ResultSetMetadata.newBuilder().addAllColumnMetadata(columnMetadataSet).build();
    selectedColumns.addAll(selectedList);
  }

  private ColumnMetadata toColumnMetadata(Expression expression) {
    ColumnMetadata.Builder builder = ColumnMetadata.newBuilder();
    ValueCase valueCase = expression.getValueCase();
    switch (valueCase) {
      case COLUMNIDENTIFIER:
        ColumnIdentifier columnIdentifier = expression.getColumnIdentifier();
        String alias = columnIdentifier.getAlias();
        if (alias != null && alias.trim().length() > 0) {
          builder.setColumnName(alias);
        } else {
          builder.setColumnName(columnIdentifier.getColumnName());
        }
        builder.setValueType(ValueType.STRING);
        builder.setIsRepeated(false);
        break;
      case FUNCTION:
        Function function = expression.getFunction();
        alias = function.getAlias();
        if (alias != null && alias.trim().length() > 0) {
          builder.setColumnName(alias);
        } else {
          // todo: handle recursive functions max(rollup(time,50)
          // workaround is to use alias for now
          builder.setColumnName(function.getFunctionName());
        }
        builder.setValueType(ValueType.STRING);
        builder.setIsRepeated(false);
        break;
      case LITERAL:
      case ORDERBY:
      case VALUE_NOT_SET:
        break;
    }
    return builder.build();
  }

  private void extractColumns(List<String> columns, Expression expression) {
    ValueCase valueCase = expression.getValueCase();
    switch (valueCase) {
      case COLUMNIDENTIFIER:
        ColumnIdentifier columnIdentifier = expression.getColumnIdentifier();
        columns.add(columnIdentifier.getColumnName());
        break;
      case LITERAL:
        // no columns
        break;
      case FUNCTION:
        Function function = expression.getFunction();
        for (Expression childExpression : function.getArgumentsList()) {
          extractColumns(columns, childExpression);
        }
        break;
      case ORDERBY:
        OrderByExpression orderBy = expression.getOrderBy();
        extractColumns(columns, orderBy.getExpression());
        break;
      case VALUE_NOT_SET:
        break;
    }
  }

  private Duration parseDuration(String timeSeriesPeriod) {
    String[] splitPeriodString = timeSeriesPeriod.split(":");
    long amount = Long.parseLong(splitPeriodString[0]);
    ChronoUnit unit = TimeUnit.valueOf(splitPeriodString[1]).toChronoUnit();
    return Duration.of(amount, unit);
  }

  private Optional<Duration> findTimeRangeDuration(Filter filter, String timeFilterColumn) {

    // time filter will always be present with AND operator
    if (filter.getOperator() != Operator.AND) {
      return Optional.empty();
    }

    Optional<Long> timeRangeStart =
        filter.getChildFilterList().stream()
            .filter(
                childFilter ->
                    this.isMatchingFilter(
                        childFilter, timeFilterColumn, List.of(Operator.GE, Operator.GT)))
            .map(matchingFilter -> matchingFilter.getRhs().getLiteral().getValue().getLong())
            .findFirst();

    Optional<Long> timeRangeEnd =
        filter.getChildFilterList().stream()
            .filter(
                childFilter ->
                    this.isMatchingFilter(
                        childFilter, timeFilterColumn, List.of(Operator.LT, Operator.LE)))
            .map(matchingFilter -> matchingFilter.getRhs().getLiteral().getValue().getLong())
            .findFirst();

    if (timeRangeStart.isPresent() && timeRangeEnd.isPresent()) {
      return Optional.of(Duration.ofMillis(timeRangeEnd.get() - timeRangeStart.get()));
    }

    return filter.getChildFilterList().stream()
        .map(childFilter -> this.findTimeRangeDuration(childFilter, timeFilterColumn))
        .flatMap(Optional::stream)
        .findFirst();
  }

  private boolean isMatchingFilter(Filter filter, String column, Collection<Operator> operators) {
    return column.equals(filter.getLhs().getColumnIdentifier().getColumnName())
        && (operators.stream()
            .anyMatch(operator -> Objects.equals(operator, filter.getOperator())));
  }

  public void computeTimeRangeDuration(QueryRequest queryRequest) {
    this.timeRangeDuration = findTimeRangeDuration(queryRequest.getFilter(), this.timeFilterColumn);
  }

  public void setTimeFilterColumn(String timeFilterColumn) {
    this.timeFilterColumn = timeFilterColumn;
  }

  public String getTenantId() {
    return this.tenantId;
  }

  public Set<String> getReferencedColumns() {
    return referencedColumns;
  }

  public ResultSetMetadata getResultSetMetadata() {
    return resultSetMetadata;
  }

  public LinkedHashSet<String> getSelectedColumns() {
    return selectedColumns;
  }

  public LinkedHashSet<Expression> getAllSelections() {
    return this.allSelections;
  }

  public Optional<Duration> getTimeSeriesPeriod() {
    return this.timeSeriesPeriod;
  }

  public Optional<Duration> getTimeRangeDuration() {
    return this.timeRangeDuration;
  }
}
