package org.hypertrace.core.query.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.core.query.service.QueryServiceImplConfig.ClientConfig;
import org.hypertrace.core.query.service.QueryServiceImplConfig.RequestHandlerConfig;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.QueryRequest.Builder;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.pinot.PinotBasedRequestHandler;
import org.hypertrace.core.query.service.pinot.ViewDefinition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryServiceImplConfigTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryServiceImplConfigTest.class);
  private Config appConfig;
  private QueryServiceImplConfig queryServiceConfig;

  @BeforeEach
  public void setup() {
    appConfig =
        ConfigFactory.parseFile(
            new File(
                QueryServiceImplConfigTest.class
                    .getClassLoader()
                    .getResource("application.conf")
                    .getPath()));
    queryServiceConfig = QueryServiceImplConfig.parse(appConfig.getConfig("service.config"));
  }

  @Test
  public void testQueryServiceImplConfigParser() {
    // Test QueryServiceImplConfig
    assertEquals("query-service", appConfig.getString("service.name"));
    assertEquals(8091, appConfig.getInt("service.admin.port"));
    assertEquals(8090, appConfig.getInt("service.port"));
    assertEquals(4, queryServiceConfig.getQueryRequestHandlersConfig().size());
    assertEquals(2, queryServiceConfig.getClients().size());

    LOGGER.info("{}", queryServiceConfig.getQueryRequestHandlersConfig());

    RequestHandlerConfig handler0 =
        RequestHandlerConfig.parse(queryServiceConfig.getQueryRequestHandlersConfig().get(0));
    assertEquals("trace-view-handler", handler0.getName());
    assertEquals("pinot", handler0.getType());
    Config requestHandlerInfo = handler0.getRequestHandlerInfo();
    LOGGER.info("{}", requestHandlerInfo);

    String tenantColumnName = "tenant_id";
    ViewDefinition viewDefinition =
        ViewDefinition.parse(requestHandlerInfo.getConfig("viewDefinition"), tenantColumnName);
    assertEquals("RawTraceView", viewDefinition.getViewName());
    assertEquals(tenantColumnName, viewDefinition.getTenantIdColumn());

    Map<String, ClientConfig> clientConfigMap =
        queryServiceConfig.getClients().stream()
            .map(ClientConfig::parse)
            .collect(Collectors.toMap(ClientConfig::getType, clientConfig -> clientConfig));
    ClientConfig clientConfig0 = clientConfigMap.get(handler0.getClientConfig());
    assertEquals("broker", clientConfig0.getType());
    assertEquals("pinotCluster0:8099", clientConfig0.getConnectionString());

    RequestHandlerConfig handler1 =
        RequestHandlerConfig.parse(queryServiceConfig.getQueryRequestHandlersConfig().get(1));
    assertEquals("span-event-view-handler", handler1.getName());
    assertEquals("pinot", handler1.getType());
    ClientConfig clientConfig1 = clientConfigMap.get(handler1.getClientConfig());
    assertEquals("zookeeper", clientConfig1.getType());
    assertEquals("pinotCluster1:2181", clientConfig1.getConnectionString());
  }

  @Test
  public void testHandlerSelection() {
    // Register all the handlers with the registry.
    for (Config config : queryServiceConfig.getQueryRequestHandlersConfig()) {
      RequestHandlerConfig handlerConfig = RequestHandlerConfig.parse(config);
      RequestHandlerRegistry.get()
          .register(
              handlerConfig.getName(),
              new RequestHandlerInfo(
                  handlerConfig.getName(), PinotBasedRequestHandler.class, handlerConfig.getRequestHandlerInfo()));
    }

    RequestHandlerSelector selector = new RequestHandlerSelector(RequestHandlerRegistry.get());

    QueryRequest queryRequest = buildSimpleQuery();
    RequestAnalyzer analyzer = new RequestAnalyzer(queryRequest);
    analyzer.analyze();
    RequestHandler handler = selector.select(queryRequest, analyzer);
    assertEquals("span-event-view-handler", handler.getName());
  }

  private QueryRequest buildSimpleQuery() {
    Builder builder = QueryRequest.newBuilder();
    ColumnIdentifier eventId = ColumnIdentifier.newBuilder().setColumnName("EVENT.id").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(eventId).build());

    ColumnIdentifier eventType = ColumnIdentifier.newBuilder().setColumnName("EVENT.type").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(eventType).build());

    ColumnIdentifier displayName =
        ColumnIdentifier.newBuilder().setColumnName("EVENT.displaySpanName").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(displayName).build());

    ColumnIdentifier tags = ColumnIdentifier.newBuilder().setColumnName("EVENT.spanTags").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(tags).build());

    Filter startTimeFilter = createFilter("EVENT.startTime", Operator.GT,
        String.valueOf(System.currentTimeMillis() - 1000 * 60 * 60 * 24));
    Filter endTimeFilter = createFilter("EVENT.endTime", Operator.LT,
        String.valueOf(System.currentTimeMillis()));
    Filter entrySpanFilter = createFilter("EVENT.isEntrySpan", Operator.EQ, "true");

    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .addChildFilter(entrySpanFilter)
            .build();
    builder.setFilter(andFilter);
    return builder.build();
  }

  private Filter createFilter(String columnName, Operator op, String value) {

    ColumnIdentifier startTimeColumn =
        ColumnIdentifier.newBuilder().setColumnName(columnName).build();
    Expression lhs = Expression.newBuilder().setColumnIdentifier(startTimeColumn).build();

    LiteralConstant constant =
        LiteralConstant.newBuilder().setValue(Value.newBuilder().setString(value).build()).build();
    Expression rhs = Expression.newBuilder().setLiteral(constant).build();
    return Filter.newBuilder().setLhs(lhs).setOperator(op).setRhs(rhs).build();
  }
}
