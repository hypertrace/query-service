package org.hypertrace.core.query.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
  private String timeFilterColumn;
  private Duration timeRangeDuration = Duration.ZERO;
  // Contains all selections to be made in the DB: selections on group by, single columns and
  // aggregations in that order.
  // There should be a one-to-one mapping between this and the columnMetadataSet in
  // ResultSetMetadata.
  // The difference between this and selectedColumns above is that this is a set of Expressions
  // while the selectedColumns
  // is a set of column names.
  private final LinkedHashSet<Expression> allSelections;
  private final Duration timeSeriesPeriod;

  public ExecutionContext(String tenantId, QueryRequest request) {
    this.tenantId = tenantId;
    this.selectedColumns = new LinkedHashSet<>();
    this.allSelections = new LinkedHashSet<>();
    this.timeSeriesPeriod = setTimeSeriesPeriod(request);
    analyze(request);
  }

  private Duration setTimeSeriesPeriod(QueryRequest request) {
    Duration period = Duration.ZERO;
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
          period = getPeriodInDuration(timeSeriesPeriod);
        }
      }
    }
    return period;
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

  private Duration getPeriodInDuration(String timeSeriesPeriod) {
    String period = timeSeriesPeriod.split("[:]")[0];
    long periodInSeconds;
    // add cases for other units when added
    switch (timeSeriesPeriod.split("[:]")[1]) {
      case "MILLISECONDS":
        periodInSeconds = Long.parseLong(period) / 1000;
        break;
      default:
        periodInSeconds = Long.parseLong(period);
    }
    return Duration.ofSeconds(periodInSeconds);
  }

  private void treeTraversal(
      Filter filter, AtomicLong lessThanFilter, AtomicLong greaterThanFilter) {

    // if filter already found, then return
    if (lessThanFilter.longValue() >= 0 && greaterThanFilter.longValue() >= 0) {
      return;
    }

    String columnName = filter.getLhs().getColumnIdentifier().getColumnName();
    if (columnName.equals(this.timeFilterColumn)) {

      long val = filter.getRhs().getLiteral().getValue().getLong();
      Operator operator = filter.getOperator();

      if (operator == Operator.GE || operator == Operator.GT) {
        greaterThanFilter.set(val);
      } else if (operator == Operator.LE || operator == Operator.LT) {
        lessThanFilter.set(val);
      }
    }

    for (Filter childFilter : filter.getChildFilterList()) {
      treeTraversal(childFilter, lessThanFilter, greaterThanFilter);
    }
  }

  public void computeTimeRangeDuration(QueryRequest request) {
    AtomicLong lessThanFilter = new AtomicLong(-1);
    AtomicLong greaterThanFilter = new AtomicLong(-1);

    if (request.getFilter().getChildFilterCount() > 0) {
      for (Filter filter : request.getFilter().getChildFilterList()) {
        treeTraversal(filter, lessThanFilter, greaterThanFilter);
      }
    }

    if (lessThanFilter.longValue() >= 0 && greaterThanFilter.longValue() >= 0) {
      this.timeRangeDuration =
          Duration.ofSeconds(
              TimeUnit.SECONDS.convert(
                  Math.abs(lessThanFilter.longValue() - greaterThanFilter.longValue()),
                  TimeUnit.MILLISECONDS));
    }
  }

  public void setTimeFilterColumn(Optional<String> timeFilterColumn) {
    this.timeFilterColumn = timeFilterColumn.orElse(null);
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
    return this.timeSeriesPeriod.isZero() ? Optional.empty() : Optional.of(this.timeSeriesPeriod);
  }

  public Duration getTimeRangeDuration() {
    return this.timeRangeDuration;
  }
}
