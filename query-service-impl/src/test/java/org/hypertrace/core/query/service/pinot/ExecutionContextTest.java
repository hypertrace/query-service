package org.hypertrace.core.query.service.pinot;

import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createFilter;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createTimeColumnGroupByExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createTimeFilter;
import static org.hypertrace.core.query.service.QueryRequestUtil.createSimpleAttributeExpression;
import static org.hypertrace.core.query.service.QueryRequestUtil.createStringLiteralValueExpression;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.QueryTimeRange;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.QueryRequest.Builder;
import org.hypertrace.core.query.service.api.ResultSetMetadata;
import org.hypertrace.core.query.service.api.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutionContextTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionContextTest.class);

  @Test
  public void testRepeatedColumns() {
    Builder builder = QueryRequest.newBuilder();
    // agg function with alias
    Function count =
        Function.newBuilder()
            .setFunctionName("Count")
            .setAlias("myCountAlias")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("Trace.id")))
            .build();
    builder.addAggregation(Expression.newBuilder().setFunction(count));

    // agg function without alias
    Function minFunction =
        Function.newBuilder()
            .setFunctionName("MIN")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("Trace.duration")))
            .build();
    builder.addAggregation(Expression.newBuilder().setFunction(minFunction));

    builder.addSelection(
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder().setColumnName("Trace.transaction_name")));

    builder.addSelection(
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder().setColumnName("Trace.transaction_name")));

    builder.addGroupBy(
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder().setColumnName("Trace.transaction_name")));
    QueryRequest queryRequest = builder.build();

    ExecutionContext context = new ExecutionContext("test", queryRequest);
    ResultSetMetadata resultSetMetadata = context.getResultSetMetadata();
    System.out.println("resultSetMetadata = " + resultSetMetadata);

    assertNotNull(resultSetMetadata);
    assertEquals(3, resultSetMetadata.getColumnMetadataCount());
    assertEquals("Trace.transaction_name", resultSetMetadata.getColumnMetadata(0).getColumnName());
    assertEquals("myCountAlias", resultSetMetadata.getColumnMetadata(1).getColumnName());
    assertEquals("MIN", resultSetMetadata.getColumnMetadata(2).getColumnName());

    // Selections should correspond in size and order to the
    // resultSetMetadata.getColumnMetadataList()
    assertEquals(3, context.getAllSelections().size());
    Iterator<Expression> selectionsIterator = context.getAllSelections().iterator();
    assertEquals(
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder().setColumnName("Trace.transaction_name"))
            .build(),
        selectionsIterator.next());
    assertEquals(Expression.newBuilder().setFunction(count).build(), selectionsIterator.next());
    assertEquals(
        Expression.newBuilder().setFunction(minFunction).build(), selectionsIterator.next());
  }

  @Test
  public void testFiltersWithLiterals() {
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder().setColumnName("Trace.transaction_name")));
    Expression expression =
        Expression.newBuilder()
            .setLiteral(LiteralConstant.newBuilder().setValue(Value.newBuilder().setString("test")))
            .build();
    builder.setFilter(
        Filter.newBuilder()
            .setLhs(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("Trace.transaction_name")))
            .setRhs(expression)
            .setOperator(Operator.EQ));

    QueryRequest queryRequest = builder.build();

    ExecutionContext context = new ExecutionContext("test", queryRequest);
    ResultSetMetadata resultSetMetadata = context.getResultSetMetadata();
    LOGGER.info("resultSetMetadata = " + resultSetMetadata);

    assertNotNull(resultSetMetadata);
    assertEquals(1, resultSetMetadata.getColumnMetadataCount());
    assertEquals("Trace.transaction_name", resultSetMetadata.getColumnMetadata(0).getColumnName());

    // Selections should correspond in size and order to the
    // resultSetMetadata.getColumnMetadataList()
    assertEquals(1, context.getAllSelections().size());
    Iterator<Expression> selectionsIterator = context.getAllSelections().iterator();
    assertEquals(
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder().setColumnName("Trace.transaction_name"))
            .build(),
        selectionsIterator.next());
  }

  @Test
  public void testReferencedColumns() {
    long startTimeInMillis = TimeUnit.MILLISECONDS.convert(Duration.ofHours(24));
    Builder builder = QueryRequest.newBuilder();
    builder.addSelection(
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder().setColumnName("Trace.transaction_name")));
    Expression expression =
        Expression.newBuilder()
            .setLiteral(LiteralConstant.newBuilder().setValue(Value.newBuilder().setString("test")))
            .build();
    Filter.Builder idFilter =
        Filter.newBuilder()
            .setLhs(
                Expression.newBuilder()
                    .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("Trace.id")))
            .setRhs(expression)
            .setOperator(Operator.EQ);
    Filter startTimeFilter =
        createTimeFilter("Trace.start_time_millis", Operator.GT, startTimeInMillis);
    Filter endTimeFilter =
        createTimeFilter(
            "Trace.end_time_millis",
            Operator.LT,
            startTimeInMillis + Duration.ofHours(24).toMillis());

    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .addChildFilter(idFilter)
            .build();
    builder.setFilter(andFilter);

    QueryRequest queryRequest = builder.build();

    ExecutionContext context = new ExecutionContext("test", queryRequest);

    Set<String> selectedColumns = context.getSelectedColumns();
    assertNotNull(selectedColumns);
    assertEquals(1, selectedColumns.size());
    assertEquals("Trace.transaction_name", selectedColumns.iterator().next());

    Set<String> referencedColumns = context.getReferencedColumns();
    assertNotNull(referencedColumns);
    assertEquals(4, referencedColumns.size());
    assertEquals(
        ImmutableSet.of(
            "Trace.transaction_name",
            "Trace.id",
            "Trace.start_time_millis",
            "Trace.end_time_millis"),
        referencedColumns);

    ResultSetMetadata resultSetMetadata = context.getResultSetMetadata();
    assertNotNull(resultSetMetadata);
    assertEquals(1, resultSetMetadata.getColumnMetadataCount());
    assertEquals("Trace.transaction_name", resultSetMetadata.getColumnMetadata(0).getColumnName());

    // Selections should correspond in size and order to the
    // resultSetMetadata.getColumnMetadataList()
    assertEquals(1, context.getAllSelections().size());
    Iterator<Expression> selectionsIterator = context.getAllSelections().iterator();
    assertEquals(
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder().setColumnName("Trace.transaction_name"))
            .build(),
        selectionsIterator.next());
  }

  @Test
  public void testSelectionsLinkedHashSet() {
    Builder builder = QueryRequest.newBuilder();
    // agg function with alias
    Function count =
        Function.newBuilder()
            .setFunctionName("Count")
            .setAlias("myCountAlias")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("Trace.id")))
            .build();
    builder.addAggregation(Expression.newBuilder().setFunction(count));

    // agg function without alias
    Function minFunction =
        Function.newBuilder()
            .setFunctionName("MIN")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("Trace.duration")))
            .build();
    builder.addAggregation(Expression.newBuilder().setFunction(minFunction));

    // Add some selections
    builder.addSelection(
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder().setColumnName("Trace.transaction_name")));
    builder.addSelection(
        Expression.newBuilder()
            .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("Trace.id")));

    // An function added into selections list is treated as a selection
    Function avg =
        Function.newBuilder()
            .setFunctionName("AVG")
            .setAlias("myAvgAlias")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("Trace.duration")))
            .build();
    builder.addSelection(Expression.newBuilder().setFunction(avg));

    // Add some group bys
    builder.addGroupBy(
        Expression.newBuilder()
            .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("Trace.api_name")));
    builder.addGroupBy(
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder().setColumnName("Trace.service_name")));
    QueryRequest queryRequest = builder.build();

    ExecutionContext context = new ExecutionContext("test", queryRequest);

    // The order in resultSetMetadata.getColumnMetadataList() and selections is group bys,
    // selections then aggregations
    ResultSetMetadata resultSetMetadata = context.getResultSetMetadata();

    assertNotNull(resultSetMetadata);
    assertEquals(7, resultSetMetadata.getColumnMetadataCount());
    assertEquals("Trace.api_name", resultSetMetadata.getColumnMetadata(0).getColumnName());
    assertEquals("Trace.service_name", resultSetMetadata.getColumnMetadata(1).getColumnName());
    assertEquals("Trace.transaction_name", resultSetMetadata.getColumnMetadata(2).getColumnName());
    assertEquals("Trace.id", resultSetMetadata.getColumnMetadata(3).getColumnName());
    assertEquals("myAvgAlias", resultSetMetadata.getColumnMetadata(4).getColumnName());
    assertEquals("myCountAlias", resultSetMetadata.getColumnMetadata(5).getColumnName());
    assertEquals("MIN", resultSetMetadata.getColumnMetadata(6).getColumnName());

    // Selections should correspond in size and order to the
    // resultSetMetadata.getColumnMetadataList()
    assertEquals(7, context.getAllSelections().size());
    Iterator<Expression> selectionsIterator = context.getAllSelections().iterator();
    assertEquals(
        Expression.newBuilder()
            .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("Trace.api_name"))
            .build(),
        selectionsIterator.next());
    assertEquals(
        Expression.newBuilder()
            .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("Trace.service_name"))
            .build(),
        selectionsIterator.next());
    assertEquals(
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder().setColumnName("Trace.transaction_name"))
            .build(),
        selectionsIterator.next());
    assertEquals(
        Expression.newBuilder()
            .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("Trace.id"))
            .build(),
        selectionsIterator.next());
    assertEquals(Expression.newBuilder().setFunction(avg).build(), selectionsIterator.next());
    assertEquals(Expression.newBuilder().setFunction(count).build(), selectionsIterator.next());
    assertEquals(
        Expression.newBuilder().setFunction(minFunction).build(), selectionsIterator.next());
  }

  @Test
  public void testSelectionsLinkedHashSetWithAttributeExpression() {
    Builder builder = QueryRequest.newBuilder();
    // agg function with alias
    Function count =
        Function.newBuilder()
            .setFunctionName("Count")
            .setAlias("myCountAlias")
            .addArguments(createSimpleAttributeExpression("Trace.id"))
            .build();
    builder.addAggregation(Expression.newBuilder().setFunction(count));

    // agg function without alias
    Function minFunction =
        Function.newBuilder()
            .setFunctionName("MIN")
            .addArguments(createSimpleAttributeExpression("Trace.duration"))
            .build();
    builder.addAggregation(Expression.newBuilder().setFunction(minFunction));

    // Add some selections
    builder.addSelection(createSimpleAttributeExpression("Trace.transaction_name"));
    builder.addSelection(createSimpleAttributeExpression("Trace.id"));

    // A function added into selections list is treated as a selection
    Function avg =
        Function.newBuilder()
            .setFunctionName("AVG")
            .setAlias("myAvgAlias")
            .addArguments(createSimpleAttributeExpression("Trace.duration"))
            .build();
    builder.addSelection(Expression.newBuilder().setFunction(avg));

    // Add some group bys
    builder.addGroupBy(createSimpleAttributeExpression("Trace.api_name"));
    builder.addGroupBy(createSimpleAttributeExpression("Trace.service_name"));
    QueryRequest queryRequest = builder.build();

    ExecutionContext context = new ExecutionContext("test", queryRequest);

    // The order in resultSetMetadata.getColumnMetadataList() and selections is group bys,
    // selections then aggregations
    ResultSetMetadata resultSetMetadata = context.getResultSetMetadata();

    assertNotNull(resultSetMetadata);
    assertEquals(7, resultSetMetadata.getColumnMetadataCount());
    assertEquals("Trace.api_name", resultSetMetadata.getColumnMetadata(0).getColumnName());
    assertEquals("Trace.service_name", resultSetMetadata.getColumnMetadata(1).getColumnName());
    assertEquals("Trace.transaction_name", resultSetMetadata.getColumnMetadata(2).getColumnName());
    assertEquals("Trace.id", resultSetMetadata.getColumnMetadata(3).getColumnName());
    assertEquals("myAvgAlias", resultSetMetadata.getColumnMetadata(4).getColumnName());
    assertEquals("myCountAlias", resultSetMetadata.getColumnMetadata(5).getColumnName());
    assertEquals("MIN", resultSetMetadata.getColumnMetadata(6).getColumnName());

    // Selections should correspond in size and order to the
    // resultSetMetadata.getColumnMetadataList()
    assertEquals(7, context.getAllSelections().size());
    Iterator<Expression> selectionsIterator = context.getAllSelections().iterator();
    assertEquals(
        createSimpleAttributeExpression("Trace.api_name").build(), selectionsIterator.next());
    assertEquals(
        createSimpleAttributeExpression("Trace.service_name").build(), selectionsIterator.next());
    assertEquals(
        createSimpleAttributeExpression("Trace.transaction_name").build(),
        selectionsIterator.next());
    assertEquals(createSimpleAttributeExpression("Trace.id").build(), selectionsIterator.next());
    assertEquals(Expression.newBuilder().setFunction(avg).build(), selectionsIterator.next());
    assertEquals(Expression.newBuilder().setFunction(count).build(), selectionsIterator.next());
    assertEquals(
        Expression.newBuilder().setFunction(minFunction).build(), selectionsIterator.next());
  }

  @Test
  public void testSetTimeSeriesPeriod() {

    // no group by
    Builder builder = QueryRequest.newBuilder();
    QueryRequest queryRequest = builder.build();
    ExecutionContext context = new ExecutionContext("test", queryRequest);
    assertEquals(Optional.empty(), context.getTimeSeriesPeriod());

    // group by on column
    builder.addGroupBy(
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder().setColumnName("Trace.service_name")));
    queryRequest = builder.build();
    context = new ExecutionContext("test", queryRequest);
    assertEquals(Optional.empty(), context.getTimeSeriesPeriod());

    // group by on other functions
    Function count =
        Function.newBuilder()
            .setFunctionName("Count")
            .setAlias("myCountAlias")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("Trace.id")))
            .build();
    builder.addGroupBy(Expression.newBuilder().setFunction(count));
    queryRequest = builder.build();
    context = new ExecutionContext("test", queryRequest);
    assertEquals(Optional.empty(), context.getTimeSeriesPeriod());
  }

  @Test
  public void testSetTimeSeriesPeriodInSecondsForTimeSeriesRequest() {
    QueryRequest queryRequest =
        QueryRequest.newBuilder()
            .addGroupBy(createTimeColumnGroupByExpression("SERVICE.startTime", "15:SECONDS"))
            .build();
    ExecutionContext context = new ExecutionContext("test", queryRequest);
    assertEquals(Optional.of(Duration.ofSeconds(15)), context.getTimeSeriesPeriod());
  }

  @Test
  public void testSetTimeSeriesPeriodInMillisecondsForTimeSeriesRequest() {
    QueryRequest queryRequest =
        QueryRequest.newBuilder()
            .addGroupBy(
                createTimeColumnGroupByExpression("SERVICE.startTime", "15000:MILLISECONDS"))
            .build();
    ExecutionContext context = new ExecutionContext("test", queryRequest);
    assertEquals(Optional.of(Duration.ofSeconds(15)), context.getTimeSeriesPeriod());
  }

  @Test
  public void testEmptyGetTimeRangeDuration() {
    QueryRequest queryRequest =
        QueryRequest.newBuilder()
            .setFilter(
                Filter.newBuilder()
                    .setOperator(Operator.OR)
                    .addChildFilter(
                        createTimeFilter(
                            "SERVICE.startTime",
                            Operator.GE,
                            TimeUnit.MILLISECONDS.convert(Duration.ofHours(1))))
                    .build())
            .build();
    ExecutionContext context = new ExecutionContext("test", queryRequest);
    context.setTimeFilterColumn("SERVICE.startTime");
    assertEquals(Optional.empty(), context.getQueryTimeRange());
  }

  @ParameterizedTest
  @MethodSource("provideQueryRequest")
  public void testGetTimeRangeDuration(QueryRequest queryRequest) {
    ExecutionContext context = new ExecutionContext("test", queryRequest);
    context.setTimeFilterColumn("SERVICE.startTime");
    assertEquals(
        Optional.of(Duration.ofMinutes(60)),
        context.getQueryTimeRange().map(QueryTimeRange::getDuration));
  }

  private static Stream<Arguments> provideQueryRequest() {

    long startTimeInMillis = TimeUnit.MILLISECONDS.convert(Duration.ofHours(1));
    long endTimeInMillis = startTimeInMillis + Duration.ofMinutes(60).toMillis();

    Filter startTimeFilter = createTimeFilter("SERVICE.startTime", Operator.GE, startTimeInMillis);
    Filter endTimeFilter = createTimeFilter("SERVICE.startTime", Operator.LT, endTimeInMillis);
    Filter idFilter =
        createFilter("SERVICE.id", Operator.NEQ, createStringLiteralValueExpression(""));

    Filter filter1 =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .addChildFilter(idFilter)
            .build();

    Filter filter2 =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(idFilter)
            .addChildFilter(
                Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addChildFilter(startTimeFilter)
                    .addChildFilter(endTimeFilter)
                    .build())
            .build();

    return Stream.of(
        Arguments.arguments(getQueryRequestWithFilter(filter1)),
        Arguments.arguments(getQueryRequestWithFilter(filter2)));
  }

  private static QueryRequest getQueryRequestWithFilter(Filter filter) {
    Builder builder = QueryRequest.newBuilder();
    builder.addGroupBy(createTimeColumnGroupByExpression("SERVICE.startTime", "15:SECONDS"));
    builder.setFilter(filter);
    return builder.build();
  }
}
