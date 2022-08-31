package org.hypertrace.core.query.service.postgres.converters;

import static org.hypertrace.core.query.service.QueryFunctionConstants.DATE_TIME_CONVERT;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_AVG;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_AVGRATE;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_CONCAT;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_COUNT;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_DISTINCTCOUNT;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_PERCENTILE;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;
import org.hypertrace.core.query.service.postgres.TableDefinition;

public class PostgresFunctionConverter {
  private static final String CONCAT_FUNCTION = "CONCATSKIPNULL";
  private static final String DEFAULT_AVG_RATE_SIZE = "PT1S";
  private static final String SUM_FUNCTION = "SUM";

  private final TableDefinition tableDefinition;
  private final PostgresFunctionConverterConfig config;

  public PostgresFunctionConverter(
      TableDefinition tableDefinition, PostgresFunctionConverterConfig config) {
    this.tableDefinition = tableDefinition;
    this.config = config;
  }

  public PostgresFunctionConverter(TableDefinition tableDefinition) {
    this(tableDefinition, new PostgresFunctionConverterConfig());
  }

  public String convert(
      PostgresExecutionContext postgresExecutionContext,
      Function function,
      java.util.function.Function<Expression, String> argumentConverter) {
    switch (function.getFunctionName().toUpperCase()) {
      case QUERY_FUNCTION_COUNT:
        return this.convertCount();
      case QUERY_FUNCTION_PERCENTILE:
        return this.toPercentile(function, argumentConverter);
      case QUERY_FUNCTION_DISTINCTCOUNT:
      case "DISTINCTCOUNTMV":
        return this.toDistinctCount(function, argumentConverter, postgresExecutionContext);
      case QUERY_FUNCTION_CONCAT:
        return this.functionToString(this.toConcat(function), argumentConverter);
      case QUERY_FUNCTION_AVG:
        return this.toAvg(function, argumentConverter);
      case QUERY_FUNCTION_AVGRATE:
        // Average rate is not supported directly in Postgres. So average rate is computed by
        // summing over all values and then dividing by a constant.
        return this.functionToStringForAvgRate(
            function, argumentConverter, postgresExecutionContext);
      case DATE_TIME_CONVERT:
        return this.functionToDateTimeConvert(function, argumentConverter);
      default:
        // TODO remove once postgres-specific logic removed from gateway - this normalization
        // reverts that logic
        if (this.isHardcodedPercentile(function)) {
          return this.convert(
              postgresExecutionContext,
              this.normalizeHardcodedPercentile(function),
              argumentConverter);
        }
        return this.functionToString(function, argumentConverter);
    }
  }

  private String functionToDateTimeConvert(
      Function function, java.util.function.Function<Expression, String> argumentConverter) {
    List<Expression> argumentsList = function.getArgumentsList();
    if (argumentsList.size() < 4) {
      throw new IllegalArgumentException("Expected four arguments : " + function);
    }
    return String.format(
        "%s(%s, %d)",
        config.getDateTimeConvertFunction(),
        argumentConverter.apply(argumentsList.get(0)),
        getTimeInMillis(argumentsList.get(3).getLiteral().getValue().getString()));
  }

  private long getTimeInMillis(String period) {
    int index = period.indexOf(":");
    if (index == -1) {
      throw new IllegalArgumentException("Unable to parse period : " + period);
    }
    int periodValue = Integer.parseInt(period.substring(0, index));
    String timeUnit = period.substring(index + 1);
    switch (timeUnit.toUpperCase()) {
      case "SECONDS":
        return TimeUnit.SECONDS.toMillis(periodValue);
      case "MINUTES":
        return TimeUnit.MINUTES.toMillis(periodValue);
      case "HOURS":
        return TimeUnit.HOURS.toMillis(periodValue);
      default:
        throw new UnsupportedOperationException("Unsupported time unit : " + timeUnit);
    }
  }

  private String functionToString(
      Function function, java.util.function.Function<Expression, String> argumentConverter) {
    String argumentString =
        function.getArgumentsList().stream()
            .map(argumentConverter)
            .collect(Collectors.joining(","));

    return function.getFunctionName() + "(" + argumentString + ")";
  }

  private String functionToStringForAvgRate(
      Function function,
      java.util.function.Function<Expression, String> argumentConverter,
      PostgresExecutionContext postgresExecutionContext) {

    String columnName = argumentConverter.apply(function.getArgumentsList().get(0));
    String rateIntervalInIso =
        function.getArgumentsList().size() == 2
            ? function.getArgumentsList().get(1).getLiteral().getValue().getString()
            : DEFAULT_AVG_RATE_SIZE;
    long rateIntervalInSeconds = isoDurationToSeconds(rateIntervalInIso);
    ExecutionContext executionContext = postgresExecutionContext.getExecutionContext();
    long aggregateIntervalInSeconds =
        executionContext
            .getTimeSeriesPeriod()
            .or(executionContext::getTimeRangeDuration)
            .orElseThrow()
            .getSeconds();

    return String.format(
        "SUM(%s) / %s", columnName, (double) aggregateIntervalInSeconds / rateIntervalInSeconds);
  }

  private String convertCount() {
    if (tableDefinition.getCountColumnName().isPresent()) {
      return SUM_FUNCTION + "(" + tableDefinition.getCountColumnName().get() + ")";
    } else {
      return "COUNT(*)";
    }
  }

  private String toAvg(
      Function function, java.util.function.Function<Expression, String> argumentConverter) {
    Expression columnNameExpr = getColumnNameExpr(function);
    String columnName =
        columnNameExpr.hasColumnIdentifier()
            ? columnNameExpr.getColumnIdentifier().getColumnName()
            : columnNameExpr.getAttributeExpression().getAttributeId();
    boolean isTdigest = tableDefinition.isTdigestColumnType(columnName);
    if (isTdigest) {
      return this.functionToString(
          Function.newBuilder()
              .setFunctionName(this.config.getTdigestAverageAggregationFunction())
              .addArguments(columnNameExpr)
              .addArguments(literalDouble(0.001))
              .addArguments(literalDouble(0.999))
              .build(),
          argumentConverter);
    } else {
      return this.functionToString(function, argumentConverter);
    }
  }

  private String toPercentile(
      Function function, java.util.function.Function<Expression, String> argumentConverter) {
    int percentileValue =
        this.getPercentileValueFromFunction(function)
            .orElseThrow(
                () ->
                    new UnsupportedOperationException(
                        String.format(
                            "%s must include an integer convertible value as its first argument. Got: %s",
                            QUERY_FUNCTION_PERCENTILE, function.getArguments(0))));
    Expression columnNameExpr = getColumnNameExpr(function);
    String columnName =
        columnNameExpr.hasColumnIdentifier()
            ? columnNameExpr.getColumnIdentifier().getColumnName()
            : columnNameExpr.getAttributeExpression().getAttributeId();
    boolean isTdigest = tableDefinition.isTdigestColumnType(columnName);
    if (isTdigest) {
      return this.functionToString(
          Function.newBuilder(function)
              .removeArguments(0)
              .setFunctionName(this.config.getTdigestPercentileAggregationFunction())
              .addArguments(literalDouble((double) percentileValue / 100))
              .build(),
          argumentConverter);
    } else {
      return String.format(
          this.config.getPercentileAggregationFunction(),
          (double) percentileValue / 100,
          argumentConverter.apply(columnNameExpr));
    }
  }

  private Expression getColumnNameExpr(Function function) {
    return function.getArgumentsList().stream()
        .filter(Expression::hasColumnIdentifier)
        .findFirst()
        .orElseGet(
            () ->
                function.getArgumentsList().stream()
                    .filter(Expression::hasAttributeExpression)
                    .findFirst()
                    .orElseThrow(
                        () -> new IllegalArgumentException("Unable to get column name expr")));
  }

  private Function toConcat(Function function) {
    return Function.newBuilder(function).setFunctionName(CONCAT_FUNCTION).build();
  }

  private String toDistinctCount(
      Function function,
      java.util.function.Function<Expression, String> argumentConverter,
      PostgresExecutionContext postgresExecutionContext) {
    String argument = argumentConverter.apply(function.getArgumentsList().get(0));
    if (postgresExecutionContext.getColumnRequestContext().isArrayColumnType()) {
      postgresExecutionContext.addUnnestTableColumnName(argument);
    }
    return String.format(this.config.getDistinctCountAggregationFunction(), argument);
  }

  private boolean isHardcodedPercentile(Function function) {
    String functionName = function.getFunctionName().toUpperCase();
    return functionName.startsWith(QUERY_FUNCTION_PERCENTILE)
        && this.getPercentileValueFromName(functionName).isPresent();
  }

  private Function normalizeHardcodedPercentile(Function function) {
    // Verified in isHardcodedPercentile
    int percentileValue = this.getPercentileValueFromName(function.getFunctionName()).orElseThrow();
    return Function.newBuilder(function)
        .setFunctionName(QUERY_FUNCTION_PERCENTILE)
        .addArguments(0, this.literalInt(percentileValue))
        .build();
  }

  private Optional<Integer> getPercentileValueFromName(String functionName) {
    try {
      return Optional.of(
          Integer.parseInt(functionName.substring(QUERY_FUNCTION_PERCENTILE.length())));
    } catch (Throwable t) {
      return Optional.empty();
    }
  }

  private Optional<Integer> getPercentileValueFromFunction(Function percentileFunction) {
    return Optional.of(percentileFunction)
        .filter(function -> function.getArgumentsCount() > 0)
        .map(function -> function.getArguments(0))
        .map(Expression::getLiteral)
        .map(LiteralConstant::getValue)
        .flatMap(this::intFromValue);
  }

  Expression literalInt(int value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.INT).setInt(value)))
        .build();
  }

  Expression literalDouble(double value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(value)))
        .build();
  }

  Expression literalString(String value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.STRING).setString(value)))
        .build();
  }

  Optional<Integer> intFromValue(Value value) {
    switch (value.getValueType()) {
      case INT:
        return Optional.of(value.getInt());
      case LONG:
        return Optional.of(Math.toIntExact(value.getLong()));
      default:
        return Optional.empty();
    }
  }

  private static long isoDurationToSeconds(String duration) {
    try {
      return Duration.parse(duration).get(ChronoUnit.SECONDS);
    } catch (DateTimeParseException ex) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported string format for duration: %s, expects iso string format", duration));
    }
  }
}
