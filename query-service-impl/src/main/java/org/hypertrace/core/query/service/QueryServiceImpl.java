package org.hypertrace.core.query.service;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.NoSuchElementException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.QueryServiceGrpc;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
class QueryServiceImpl extends QueryServiceGrpc.QueryServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(QueryServiceImpl.class);
  private final RequestHandlerSelector selector;

  @Inject
  public QueryServiceImpl(RequestHandlerSelector selector) {
    this.selector = selector;
  }

  @Override
  public void execute(QueryRequest queryRequest, StreamObserver<ResultSetChunk> responseObserver) {
    try {
      String tenantId = RequestContext.CURRENT.get().getTenantId().get();
      ExecutionContext context = new ExecutionContext(tenantId, queryRequest);
      RequestHandler requestHandler = selector.select(queryRequest, context);
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

      requestHandler.handleRequest(context, queryRequest, collector);
    } catch (NoSuchElementException e) {
      LOG.error("TenantId is missing in the context.", e);
      responseObserver.onError(e);
    } catch (Exception e) {
      LOG.error("Error processing request: {}", queryRequest, e);
      responseObserver.onError(e);
    }
  }
}
