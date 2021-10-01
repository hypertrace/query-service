package org.hypertrace.core.query.service.pinot.converters;

import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_AVG_RATE;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_CONCAT;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_COUNT;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_PERCENTILE;

import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;

public class PinotFunctionConverter {
  /**
   * Computing PERCENTILE in Pinot is resource intensive. T-Digest calculation is much faster and
   * reasonably accurate, hence use that as the default.
   *
   * AVG_RATE not supported directly in Pinot. So AVG_RATE is computed by summing over all values
   * and then dividing by a constant (1s in this case).
   */
  private static final String DEFAULT_PERCENTILE_AGGREGATION_FUNCTION = "PERCENTILETDIGEST";

  private static final String PINOT_CONCAT_FUNCTION = "CONCATSKIPNULL";

  private final String percentileAggFunction;

  public PinotFunctionConverter(String configuredPercentileFunction) {
    this.percentileAggFunction =
        Optional.ofNullable(configuredPercentileFunction)
            .orElse(DEFAULT_PERCENTILE_AGGREGATION_FUNCTION);
  }

  public PinotFunctionConverter() {
    this.percentileAggFunction = DEFAULT_PERCENTILE_AGGREGATION_FUNCTION;
  }

  public String convert(
      ExecutionContext executionContext,
      Function function, java.util.function.Function<Expression, String> argumentConverter) {
    switch (function.getFunctionName().toUpperCase()) {
      case QUERY_FUNCTION_COUNT:
        return this.convertCount();
      case QUERY_FUNCTION_PERCENTILE:
        return this.functionToString(this.toPinotPercentile(function), argumentConverter);
      case QUERY_FUNCTION_CONCAT:
        return this.functionToString(this.toPinotConcat(function), argumentConverter);
      case QUERY_FUNCTION_AVG_RATE:
        return this.functionToStringForAvgRate(function, argumentConverter);
      default:
        // TODO remove once pinot-specific logic removed from gateway - this normalization reverts
        // that logic
        if (this.isHardcodedPercentile(function)) {
          return this.convert(executionContext, this.normalizeHardcodedPercentile(function), argumentConverter);
        }
        return this.functionToString(function, argumentConverter);
    }
  }

  private String functionToStringForAvgRate(
      Function function, java.util.function.Function<Expression, String> argumentConverter) {
    String argumentString =
        function.getArgumentsList().stream()
            .map(argumentConverter::apply)
            .collect(Collectors.joining(","));

    return "SUM(DIV(" + argumentString + "))";
  }

  private String functionToString(
      Function function, java.util.function.Function<Expression, String> argumentConverter) {
    String argumentString =
        function.getArgumentsList().stream()
            .map(argumentConverter::apply)
            .collect(Collectors.joining(","));

    return function.getFunctionName() + "(" + argumentString + ")";
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
        .setFunctionName(this.percentileAggFunction + percentileValue)
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
}
