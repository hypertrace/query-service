package org.hypertrace.core.query.service.pinot.converters;

import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_AVG;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_AVG_RATE;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_CONCAT;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_COUNT;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_DISTINCTCOUNT;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_MAX;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_MIN;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_PERCENTILE;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_SUM;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createColumnExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createIntLiteralValueExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createLongLiteralValueExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createStringLiteralValueExpression;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Expression.Builder;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PinotFunctionConverterTest {

  @Mock java.util.function.Function<Expression, String> mockArgumentConverter;
  @Mock ExecutionContext mockingExecutionContext;
  ExecutionContext testExecutionContext =
      new ExecutionContext("__default", QueryRequest.newBuilder().build());

  @Test
  void convertsCountStar() {
    String expected = "COUNT(*)";
    Function countFunction = buildFunction(QUERY_FUNCTION_COUNT, createColumnExpression("foo"));

    assertEquals(
        expected,
        new PinotFunctionConverter()
            .convert(testExecutionContext, countFunction, this.mockArgumentConverter));

    // Should not be case sensitive
    Function lowerCaseCountFunction = buildFunction("count", createColumnExpression("foo"));
    assertEquals(
        expected,
        new PinotFunctionConverter()
            .convert(testExecutionContext, lowerCaseCountFunction, this.mockArgumentConverter));
  }

  @Test
  void convertsPercentileWithValueArg() {
    String expected = "PERCENTILETDIGEST90(fooOut)";
    Expression columnExpression = createColumnExpression("fooIn").build();
    Function intPercentileFunction =
        buildFunction(
            QUERY_FUNCTION_PERCENTILE,
            createIntLiteralValueExpression(90).toBuilder(),
            columnExpression.toBuilder());
    when(this.mockArgumentConverter.apply(columnExpression)).thenReturn("fooOut");

    assertEquals(
        expected,
        new PinotFunctionConverter()
            .convert(testExecutionContext, intPercentileFunction, this.mockArgumentConverter));

    // Should not be case sensitive
    Function lowerCasePercentileFunction =
        intPercentileFunction.toBuilder().setFunctionName("percentile").build();
    assertEquals(
        expected,
        new PinotFunctionConverter()
            .convert(
                testExecutionContext, lowerCasePercentileFunction, this.mockArgumentConverter));

    // Should work with longs
    Function longPercentileFunction =
        intPercentileFunction.toBuilder()
            .setArguments(0, createLongLiteralValueExpression(90))
            .build();
    assertEquals(
        expected,
        new PinotFunctionConverter()
            .convert(testExecutionContext, longPercentileFunction, this.mockArgumentConverter));
  }

  @Test
  void acceptsCustomPercentileFunctions() {
    String expected = "CUSTOMPERCENTILE90(foo)";
    Function percentileFunction =
        buildFunction(
            QUERY_FUNCTION_PERCENTILE,
            createIntLiteralValueExpression(90).toBuilder(),
            createColumnExpression("foo"));
    when(this.mockArgumentConverter.apply(any(Expression.class))).thenReturn("foo");

    assertEquals(
        expected,
        new PinotFunctionConverter("CUSTOMPERCENTILE")
            .convert(testExecutionContext, percentileFunction, this.mockArgumentConverter));
  }

  @Test
  void errorsOnInvalidPercentileValueArg() {
    Function percentileFunctionWithoutValue =
        buildFunction(QUERY_FUNCTION_PERCENTILE, createColumnExpression("foo"));

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            new PinotFunctionConverter()
                .convert(
                    testExecutionContext,
                    percentileFunctionWithoutValue,
                    this.mockArgumentConverter));
  }

  @Test
  void backwardsCompatibleWithPercentileValueFunction() {
    String expected = "PERCENTILETDIGEST90(foo)";
    Function percentileFunction = buildFunction("PERCENTILE90", createColumnExpression("foo"));
    when(this.mockArgumentConverter.apply(any(Expression.class))).thenReturn("foo");

    assertEquals(
        expected,
        new PinotFunctionConverter()
            .convert(testExecutionContext, percentileFunction, this.mockArgumentConverter));
  }

  @Test
  void convertsBasicFunctions() {
    PinotFunctionConverter converter = new PinotFunctionConverter();

    when(this.mockArgumentConverter.apply(any(Expression.class))).thenReturn("foo");
    assertEquals(
        "SUM(foo)",
        converter.convert(
            testExecutionContext,
            buildFunction(QUERY_FUNCTION_SUM, createColumnExpression("foo")),
            this.mockArgumentConverter));

    assertEquals(
        "AVG(foo)",
        converter.convert(
            testExecutionContext,
            buildFunction(QUERY_FUNCTION_AVG, createColumnExpression("foo")),
            this.mockArgumentConverter));
    assertEquals(
        "DISTINCTCOUNT(foo)",
        converter.convert(
            testExecutionContext,
            buildFunction(QUERY_FUNCTION_DISTINCTCOUNT, createColumnExpression("foo")),
            this.mockArgumentConverter));
    assertEquals(
        "MAX(foo)",
        converter.convert(
            testExecutionContext,
            buildFunction(QUERY_FUNCTION_MAX, createColumnExpression("foo")),
            this.mockArgumentConverter));
    assertEquals(
        "MIN(foo)",
        converter.convert(
            testExecutionContext,
            buildFunction(QUERY_FUNCTION_MIN, createColumnExpression("foo")),
            this.mockArgumentConverter));
  }

  @Test
  void convertsUnknownFunctions() {
    when(this.mockArgumentConverter.apply(any(Expression.class))).thenReturn("foo");
    assertEquals(
        "UNKNOWN(foo)",
        new PinotFunctionConverter()
            .convert(
                testExecutionContext,
                buildFunction("UNKNOWN", createColumnExpression("foo")),
                this.mockArgumentConverter));
  }

  @Test
  void convertsConcatFunction() {
    Expression column1 = createColumnExpression("foo").build();
    Expression column2 = createColumnExpression("bar").build();

    when(this.mockArgumentConverter.apply(column1)).thenReturn("foo");
    when(this.mockArgumentConverter.apply(column2)).thenReturn("bar");
    assertEquals(
        "CONCATSKIPNULL(foo,bar)",
        new PinotFunctionConverter()
            .convert(
                testExecutionContext,
                buildFunction(QUERY_FUNCTION_CONCAT, column1.toBuilder(), column2.toBuilder()),
                this.mockArgumentConverter));
  }

  @Test
  void convertAvgRateFunction() {
    Expression column1 = createColumnExpression("foo").build();
    Expression column2 = createStringLiteralValueExpression("PT2S");

    when(this.mockArgumentConverter.apply(column1)).thenReturn("foo");
    when(this.mockingExecutionContext.getTimeSeriesPeriod())
        .thenReturn(Optional.of(Duration.ofSeconds(10)));

    assertEquals(
        "SUM(DIV(foo, 5.0))",
        new PinotFunctionConverter()
            .convert(
                mockingExecutionContext,
                buildFunction(QUERY_FUNCTION_AVG_RATE, column1.toBuilder(), column2.toBuilder()),
                this.mockArgumentConverter));

    when(this.mockingExecutionContext.getTimeSeriesPeriod()).thenReturn(Optional.empty());
    when(this.mockingExecutionContext.getTimeRangeDuration()).thenReturn(Duration.ofSeconds(10));

    assertEquals(
        "SUM(DIV(foo, 5.0))",
        new PinotFunctionConverter()
            .convert(
                mockingExecutionContext,
                buildFunction(QUERY_FUNCTION_AVG_RATE, column1.toBuilder(), column2.toBuilder()),
                this.mockArgumentConverter));

    when(this.mockingExecutionContext.getTimeSeriesPeriod())
        .thenReturn(Optional.of(Duration.ofSeconds(20)));
    when(this.mockingExecutionContext.getTimeRangeDuration()).thenReturn(Duration.ofSeconds(10));

    assertEquals(
        "SUM(DIV(foo, 10.0))",
        new PinotFunctionConverter()
            .convert(
                mockingExecutionContext,
                buildFunction(QUERY_FUNCTION_AVG_RATE, column1.toBuilder(), column2.toBuilder()),
                this.mockArgumentConverter));
  }

  @Test
  void testIllegalDurationFormat() {
    Expression column1 = createColumnExpression("foo").build();
    Expression column2 = createStringLiteralValueExpression("2S");

    when(this.mockArgumentConverter.apply(column1)).thenReturn("foo");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new PinotFunctionConverter()
                .convert(
                    mockingExecutionContext,
                    buildFunction(
                        QUERY_FUNCTION_AVG_RATE, column1.toBuilder(), column2.toBuilder()),
                    this.mockArgumentConverter));
  }

  private Function buildFunction(String name, Builder... arguments) {
    return Function.newBuilder()
        .setFunctionName(name)
        .addAllArguments(Arrays.stream(arguments).map(Builder::build).collect(Collectors.toList()))
        .build();
  }
}
