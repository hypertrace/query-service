package org.hypertrace.core.query.service.pinot.converters;

import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_AVGRATE;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_CONCAT;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_COUNT;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_DISTINCTCOUNT;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_PERCENTILE;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;

public class PinotFunctionConverter {
  private static final String PINOT_CONCAT_FUNCTION = "CONCATSKIPNULL";
  private static final String PINOT_DISTINCT_COUNT_MV_FUNCTION = "DISTINCTCOUNTMV";

  private static final String DEFAULT_AVG_RATE_SIZE = "PT1S";
  private final PinotFunctionConverterConfig config;

  public PinotFunctionConverter(PinotFunctionConverterConfig config) {
    this.config = config;
  }

  public PinotFunctionConverter() {
    this(new PinotFunctionConverterConfig());
  }

  public String convert(
      ExecutionContext executionContext,
      Function function,
      java.util.function.Function<Expression, String> argumentConverter) {
    switch (function.getFunctionName().toUpperCase()) {
      case QUERY_FUNCTION_COUNT:
        return this.convertCount();
      case QUERY_FUNCTION_PERCENTILE:
        // Computing PERCENTILE in Pinot is resource intensive. T-Digest calculation is much faster
        // and reasonably accurate, so support selecting the implementation to use
        return this.functionToString(this.toPinotPercentile(function), argumentConverter);
      case QUERY_FUNCTION_DISTINCTCOUNT:
        return this.functionToStringForDistinctCount(function, argumentConverter);
      case QUERY_FUNCTION_CONCAT:
        return this.functionToString(this.toPinotConcat(function), argumentConverter);
      case PINOT_DISTINCT_COUNT_MV_FUNCTION:
        return this.functionToStringForDistinctCountMv(function, argumentConverter);
      case QUERY_FUNCTION_AVGRATE:
        // AVGRATE not supported directly in Pinot. So AVG_RATE is computed by summing over all
        // values and then dividing by a constant.
        return this.functionToStringForAvgRate(function, argumentConverter, executionContext);
      default:
        // TODO remove once pinot-specific logic removed from gateway - this normalization reverts
        // that logic
        if (this.isHardcodedPercentile(function)) {
          return this.convert(
              executionContext, this.normalizeHardcodedPercentile(function), argumentConverter);
        }
        return this.functionToString(function, argumentConverter);
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

  private String functionToStringForDistinctCount(
      Function function, java.util.function.Function<Expression, String> argumentConverter) {
    String columnName = argumentConverter.apply(function.getArgumentsList().get(0));
    return this.config.getDistinctCountFunction(columnName) + "(" + columnName + ")";
  }

  private String functionToStringForDistinctCountMv(
      Function function, java.util.function.Function<Expression, String> argumentConverter) {
    String columnName = argumentConverter.apply(function.getArgumentsList().get(0));
    return this.config.getDistinctCountMvFunction(columnName) + "(" + columnName + ")";
  }

  private String functionToStringForAvgRate(
      Function function,
      java.util.function.Function<Expression, String> argumentConverter,
      ExecutionContext executionContext) {

    String columnName = argumentConverter.apply(function.getArgumentsList().get(0));
    String rateIntervalInIso =
        function.getArgumentsList().size() == 2
            ? function.getArgumentsList().get(1).getLiteral().getValue().getString()
            : DEFAULT_AVG_RATE_SIZE;
    long rateIntervalInSeconds = isoDurationToSeconds(rateIntervalInIso);
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
    return "COUNT(*)";
  }

  private Function toPinotPercentile(Function function) {
    int percentileValue =
        this.getPercentileValueFromFunction(function)
            .orElseThrow(
                () ->
                    new UnsupportedOperationException(
                        String.format(
                            "%s must include an integer convertible value as its first argument. Got: %s",
                            QUERY_FUNCTION_PERCENTILE, function.getArguments(0))));
    return Function.newBuilder(function)
        .removeArguments(0)
        .setFunctionName(this.config.getPercentileAggregationFunction() + percentileValue)
        .build();
  }

  private Function toPinotConcat(Function function) {
    // We don't want to use pinot's built in concat, it has different null behavior.
    // Instead, use our custom UDF.
    return Function.newBuilder(function).setFunctionName(PINOT_CONCAT_FUNCTION).build();
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
