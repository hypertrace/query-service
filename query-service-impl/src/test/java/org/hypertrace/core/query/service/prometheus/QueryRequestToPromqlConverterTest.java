package org.hypertrace.core.query.service.prometheus;

import static java.util.Objects.requireNonNull;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createColumnExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createFunctionExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createTimeFilter;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.LinkedHashSet;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.QueryRequest.Builder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class QueryRequestToPromqlConverterTest {

  private static final String TENANT_COLUMN_NAME = "tenant_id";

  private static final String TEST_REQUEST_HANDLER_CONFIG_FILE = "prometheus_request_handler.conf";

  @Test
  public void testQueryWithGroupByWithMultipleAggregates() {
    QueryRequest orderByQueryRequest = buildMultipleGroupByMultipleAggQuery();
    Builder builder = QueryRequest.newBuilder(orderByQueryRequest);
    builder.setLimit(20);
    PrometheusViewDefinition prometheusViewDefinition = getDefaultPrometheusViewDefinition();

    QueryRequest queryRequest = builder.build();

    ExecutionContext executionContext = new ExecutionContext("__default", queryRequest);
    executionContext.setTimeFilterColumn("SERVICE.startTime");
    PromqlQuery promqlQuery =
        new QueryRequestToPromqlConverter(prometheusViewDefinition)
            .toPromql(
                executionContext, builder.build(), createSelectionsFromQueryRequest(queryRequest));

    String query1 = "count by (service_name, api_name) (count_over_time(error_count{}[100ms]))";
    String query2 = "avg by (service_name, api_name) (avg_over_time(num_calls{}[100ms]))";

    Assertions.assertTrue(promqlQuery.getQueries().contains(query1));
    Assertions.assertTrue(promqlQuery.getQueries().contains(query2));
  }

  private QueryRequest buildMultipleGroupByMultipleAggQuery() {
    Builder builder = QueryRequest.newBuilder();
    builder.addAggregation(
        createFunctionExpression("Count", createColumnExpression("SERVICE.errorCount").build()));
    Expression avg =
        createFunctionExpression("AVG", createColumnExpression("SERVICE.numCalls").build());
    builder.addAggregation(avg);

    Filter startTimeFilter = createTimeFilter("SERVICE.startTime", Operator.GT, 100L);
    Filter endTimeFilter = createTimeFilter("SERVICE.startTime", Operator.LT, 200L);

    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .build();
    builder.setFilter(andFilter);

    builder.addGroupBy(createColumnExpression("SERVICE.name"));
    builder.addGroupBy(createColumnExpression("API.name"));
    return builder.build();
  }

  private PrometheusViewDefinition getDefaultPrometheusViewDefinition() {
    Config fileConfig =
        ConfigFactory.parseURL(
            requireNonNull(
                QueryRequestToPromqlConverterTest.class
                    .getClassLoader()
                    .getResource(TEST_REQUEST_HANDLER_CONFIG_FILE)));

    return PrometheusViewDefinition.parse(
        fileConfig.getConfig("requestHandlerInfo.prometheusViewDefinition"), TENANT_COLUMN_NAME);
  }

  private LinkedHashSet<Expression> createSelectionsFromQueryRequest(QueryRequest queryRequest) {
    LinkedHashSet<Expression> selections = new LinkedHashSet<>();

    selections.addAll(queryRequest.getGroupByList());
    selections.addAll(queryRequest.getSelectionList());
    selections.addAll(queryRequest.getAggregationList());

    return selections;
  }
}
