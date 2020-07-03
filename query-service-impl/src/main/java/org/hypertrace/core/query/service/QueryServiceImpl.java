package org.hypertrace.core.query.service;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.query.service.QueryServiceImplConfig.ClientConfig;
import org.hypertrace.core.query.service.QueryServiceImplConfig.RequestHandlerConfig;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.QueryServiceGrpc;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.pinot.PinotBasedRequestHandler;
import org.hypertrace.core.query.service.pinot.PinotClientFactory;
import org.hypertrace.core.query.service.pinot.ViewDefinition;

public class QueryServiceImpl extends QueryServiceGrpc.QueryServiceImplBase {

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(QueryServiceImpl.class);

  private final RequestHandlerSelector selector;

  public QueryServiceImpl(QueryServiceImplConfig config) {
    Map<String, ClientConfig> clientConfigMap =
        config.getClients().stream()
            .map(ClientConfig::parse)
            .collect(Collectors.toMap(ClientConfig::getType, clientConfig -> clientConfig));
    String tenantColumnName = config.getTenantColumnName();

    if (tenantColumnName == null || tenantColumnName.isBlank()) {
      throw new RuntimeException(
          "Tenant column name is not defined. Need to set service.config.tenantColumnName in the application config.");
    }

    for (Config requestHandlerConfig : config.getQueryRequestHandlersConfig()) {
      initRequestHandler(
          RequestHandlerConfig.parse(requestHandlerConfig), clientConfigMap, tenantColumnName);
    }
    selector = new RequestHandlerSelector(RequestHandlerRegistry.get());
  }

  private void initRequestHandler(
      RequestHandlerConfig config,
      Map<String, ClientConfig> clientConfigMap,
      String tenantColumnName) {

    // Register Pinot RequestHandler
    if ("pinot".equals(config.getType())) {
      Map<String, Object> requestHandlerInfoConf = new HashMap<>();
      requestHandlerInfoConf.put(
          PinotBasedRequestHandler.VIEW_DEFINITION_CONFIG_KEY,
          ViewDefinition.parse(
              (Map<String, Object>)
                  config
                      .getRequestHandlerInfo()
                      .get(PinotBasedRequestHandler.VIEW_DEFINITION_CONFIG_KEY),
              tenantColumnName));
      RequestHandlerRegistry.get()
          .register(
              config.getName(),
              new RequestHandlerInfo(
                  config.getName(), PinotBasedRequestHandler.class, requestHandlerInfoConf));
    } else {
      throw new UnsupportedOperationException(
          "Unsupported RequestHandler type - " + config.getType());
    }

    // Register Pinot Client
    ClientConfig clientConfig = clientConfigMap.get(config.getClientConfig());
    Preconditions.checkNotNull(clientConfig);
    PinotClientFactory.createPinotClient(
        config.getName(), clientConfig.getType(), clientConfig.getConnectionString());
  }

  @Override
  public void execute(QueryRequest queryRequest, StreamObserver<ResultSetChunk> responseObserver) {
    try {
      RequestAnalyzer analyzer = new RequestAnalyzer(queryRequest);
      analyzer.analyze();
      RequestHandler requestHandler = selector.select(queryRequest, analyzer);
      if (requestHandler == null) {
        // An error is logged in the select() method
        responseObserver.onError(
            Status.NOT_FOUND
                .withDescription("Could not find any handler to handle the request")
                .asException());
        return;
      }

      ResultSetChunkCollector collector = new ResultSetChunkCollector(responseObserver);
      collector.init(analyzer.getResultSetMetadata());

      String tenantId = RequestContext.CURRENT.get().getTenantId().get();
      requestHandler.handleRequest(new QueryContext(tenantId), queryRequest, collector, analyzer);
    } catch (NoSuchElementException e) {
      LOG.error("TenantId is missing in the context.", e);
      responseObserver.onError(e);
    } catch (Exception e) {
      LOG.error("Error processing request: {}", queryRequest, e);
      responseObserver.onError(e);
    }
  }
}
