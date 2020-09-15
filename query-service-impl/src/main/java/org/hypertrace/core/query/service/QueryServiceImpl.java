package org.hypertrace.core.query.service;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
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

public class QueryServiceImpl extends QueryServiceGrpc.QueryServiceImplBase {

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(QueryServiceImpl.class);

  private final RequestHandlerSelector selector;

  public QueryServiceImpl(QueryServiceImplConfig config) {
    Map<String, ClientConfig> clientConfigMap =
        config.getClients().stream()
            .map(ClientConfig::parse)
            .collect(Collectors.toMap(ClientConfig::getType, clientConfig -> clientConfig));

    for (Config requestHandlerConfig : config.getQueryRequestHandlersConfig()) {
      initRequestHandler(
          RequestHandlerConfig.parse(requestHandlerConfig), clientConfigMap);
    }
    selector = new RequestHandlerSelector(RequestHandlerRegistry.get());
  }

  private void initRequestHandler(RequestHandlerConfig config,
      Map<String, ClientConfig> clientConfigMap) {

    // Register Pinot RequestHandler
    if ("pinot".equals(config.getType())) {
      boolean registered = RequestHandlerRegistry.get().register(config.getName(),
          new RequestHandlerInfo(
              config.getName(), PinotBasedRequestHandler.class, config.getRequestHandlerInfo()));
      if (!registered) {
        throw new RuntimeException("Could not initialize the request handler: " + config.getName());
      }
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
      String tenantId = RequestContext.CURRENT.get().getTenantId().get();
      ExecutionContext context = new ExecutionContext(tenantId, queryRequest);

      // Optimize the filter in the request and rewrite query request.
      QueryRequest request = QueryRequest.newBuilder(queryRequest)
          .setFilter(QueryRequestFilterUtils.optimizeFilter(queryRequest.getFilter())).build();

      RequestHandler requestHandler = selector.select(request, context);
      if (requestHandler == null) {
        // An error is logged in the select() method
        responseObserver.onError(
            Status.NOT_FOUND
                .withDescription("Could not find any handler to handle the request")
                .asException());
        return;
      }

      ResultSetChunkCollector collector = new ResultSetChunkCollector(responseObserver);
      collector.init(context.getResultSetMetadata());

      requestHandler.handleRequest(context, request, collector);
    } catch (NoSuchElementException e) {
      LOG.error("TenantId is missing in the context.", e);
      responseObserver.onError(e);
    } catch (Exception e) {
      LOG.error("Error processing request: {}", queryRequest, e);
      responseObserver.onError(e);
    }
  }
}
