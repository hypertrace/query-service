package org.hypertrace.core.query.service.postgres;

import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createTimeFilter;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.internal.util.collections.Iterables.firstOf;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.client.ResultSet;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.QueryCost;
import org.hypertrace.core.query.service.QueryRequestBuilderUtils;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.pinot.ResultSetTypePredicateProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PostgresBasedRequestHandlerTest {
  private final Config serviceConfig =
      ConfigFactory.parseURL(
              Objects.requireNonNull(
                  QueryRequestToPostgresSQLConverterTest.class
                      .getClassLoader()
                      .getResource("application.conf")))
          .getConfig("service.config");

  @Test
  public void testCanHandle() {
    for (Config config : serviceConfig.getConfigList("queryRequestHandlersConfig")) {
      if (!isPostgresConfig(config)) {
        continue;
      }

      PostgresBasedRequestHandler handler =
          new PostgresBasedRequestHandler(
              config.getString("name"), config.getConfig("requestHandlerInfo"));

      // Verify that the traces handler can traces query.
      if (config
          .getString("name")
          .equals("backend-traces-from-bare-span-event-view-aggr-handler")) {
        QueryRequest.Builder builder = QueryRequest.newBuilder();
        long startTimeInMillis = TimeUnit.MILLISECONDS.convert(Duration.ofHours(24));
        Filter startTimeFilter =
            createTimeFilter("BACKEND_TRACE.startTime", Operator.GT, startTimeInMillis);
        Filter endTimeFilter =
            createTimeFilter(
                "BACKEND_TRACE.startTime",
                Operator.LT,
                startTimeInMillis + Duration.ofHours(1).toMillis());

        Filter andFilter =
            Filter.newBuilder()
                .setOperator(Operator.AND)
                .addChildFilter(startTimeFilter)
                .addChildFilter(endTimeFilter)
                .build();
        builder.setFilter(andFilter);
        builder.addSelection(
            QueryRequestBuilderUtils.createColumnExpression("BACKEND_TRACE.backendId"));

        QueryRequest request = builder.build();
        ExecutionContext context = new ExecutionContext("__default", request);
        QueryCost cost = handler.canHandle(request, context);
        Assertions.assertTrue(cost.getCost() >= 0.0d && cost.getCost() < 1.0d);
      }
    }
  }

  private boolean isPostgresConfig(Config config) {
    return config.getString("type").equals("postgres");
  }
}
