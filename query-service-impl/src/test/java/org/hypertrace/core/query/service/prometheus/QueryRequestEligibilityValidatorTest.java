package org.hypertrace.core.query.service.prometheus;

import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createColumnExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createFunctionExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createOrderByExpression;

import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.QueryCost;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.QueryRequest.Builder;
import org.hypertrace.core.query.service.api.SortOrder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class QueryRequestEligibilityValidatorTest {

  private QueryRequestEligibilityValidator queryRequestEligibilityValidator;

  @BeforeEach
  public void setup() {
    queryRequestEligibilityValidator =
        new QueryRequestEligibilityValidator(
            PrometheusTestUtils.getDefaultPrometheusViewDefinition());
  }

  @Test
  void testCalculateCost_orderBy() {
    QueryRequest queryRequest = buildOrderByQuery();

    ExecutionContext executionContext = new ExecutionContext("__default", queryRequest);
    executionContext.setTimeFilterColumn("SERVICE.startTime");

    Assertions.assertEquals(
        QueryCost.UNSUPPORTED,
        queryRequestEligibilityValidator.calculateCost(queryRequest, executionContext));
  }

  @Test
  void testCalculateCost_groupByAndSelectionOnDifferentColumn() {
    Builder builder = QueryRequest.newBuilder();
    Expression startTimeColumn = createColumnExpression("SERVICE.startTime").build();

    builder.addSelection(createColumnExpression("SERVICE.id"));
    builder.addSelection(startTimeColumn);
    builder.addGroupBy(createColumnExpression("SERVICE.name"));

    QueryRequest queryRequest = builder.build();

    ExecutionContext executionContext = new ExecutionContext("__default", queryRequest);
    executionContext.setTimeFilterColumn("SERVICE.startTime");

    Assertions.assertEquals(
        QueryCost.UNSUPPORTED,
        queryRequestEligibilityValidator.calculateCost(queryRequest, executionContext));
  }

  @Test
  void testCalculateCost_aggregationNotSupported() {
    Builder builder = QueryRequest.newBuilder();
    builder.addAggregation(
        createFunctionExpression("Count", createColumnExpression("SERVICE.name").build()));

    Expression startTimeColumn = createColumnExpression("SERVICE.startTime").build();

    builder.addSelection(createColumnExpression("SERVICE.id"));
    builder.addSelection(startTimeColumn);
    builder.addGroupBy(createColumnExpression("SERVICE.name"));

    QueryRequest queryRequest = builder.build();

    ExecutionContext executionContext = new ExecutionContext("__default", queryRequest);
    executionContext.setTimeFilterColumn("SERVICE.startTime");

    Assertions.assertEquals(
        QueryCost.UNSUPPORTED,
        queryRequestEligibilityValidator.calculateCost(queryRequest, executionContext));
  }

  private QueryRequest buildOrderByQuery() {
    Builder builder = QueryRequest.newBuilder();
    Expression startTimeColumn = createColumnExpression("SERVICE.startTime").build();
    Expression endTimeColumn = createColumnExpression("SERVICE.endTime").build();

    builder.addSelection(createColumnExpression("SERVICE.id"));
    builder.addSelection(startTimeColumn);
    builder.addSelection(endTimeColumn);

    builder.addOrderBy(createOrderByExpression(startTimeColumn.toBuilder(), SortOrder.DESC));
    builder.addOrderBy(createOrderByExpression(endTimeColumn.toBuilder(), SortOrder.ASC));

    builder.setLimit(100);
    return builder.build();
  }
}
