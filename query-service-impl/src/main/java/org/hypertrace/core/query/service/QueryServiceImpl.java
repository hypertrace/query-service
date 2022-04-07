package org.hypertrace.core.query.service;

import static org.hypertrace.core.query.service.RowChunkingOperator.chunkRows;

import com.google.common.collect.ImmutableMap;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Observable;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.grpcutils.server.rx.ServerCallStreamRxObserver;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.QueryServiceGrpc;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.validation.QueryValidator;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Singleton
@Slf4j
class QueryServiceImpl extends QueryServiceGrpc.QueryServiceImplBase {

  private final RequestHandlerSelector handlerSelector;
  private final QueryTransformationPipeline queryTransformationPipeline;
  private final QueryValidator queryValidator;

  private Counter serviceResponseErrorCounter;

  class QueryServiceObserver<T> extends ServerCallStreamRxObserver<T> {

    public QueryServiceObserver(StreamObserver<T> serverCallStreamObserver) {
      super(serverCallStreamObserver);
    }

    @Override
    public void onError(Throwable th) {
      serviceResponseErrorCounter.increment();
      super.onError(th);
    }

  }

  @Inject
  public QueryServiceImpl(
      RequestHandlerSelector handlerSelector,
      QueryTransformationPipeline queryTransformationPipeline,
      QueryValidator queryValidator) {
    this.handlerSelector = handlerSelector;
    this.queryTransformationPipeline = queryTransformationPipeline;
    this.queryValidator = queryValidator;
    initMetrics();
  }

  private static final String COUNTER_NAME = "hypertrace.query-service.response.errors";

  private void initMetrics() {
    serviceResponseErrorCounter =
            PlatformMetricsRegistry.registerCounter(
                    COUNTER_NAME, ImmutableMap.of());
  }

  @Override
  public void execute(
      QueryRequest originalRequest, StreamObserver<ResultSetChunk> callStreamObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    this.queryValidator
        .validate(originalRequest, requestContext)
        .andThen(
            Observable.defer(
                () ->
                    this.transformAndExecute(
                        originalRequest, requestContext.getTenantId().orElseThrow())))
        .doOnError(error -> log.error("Query failed: {}", originalRequest, error))
        .subscribe(
            new QueryServiceObserver<>(
                (ServerCallStreamObserver<ResultSetChunk>) callStreamObserver));
  }

  private Observable<ResultSetChunk> transformAndExecute(
      QueryRequest originalRequest, String tenantId) {
    return this.queryTransformationPipeline
        .transform(originalRequest, tenantId)
        .flatMapObservable(
            transformedRequest ->
                this.executeTransformedRequest(
                    transformedRequest, new ExecutionContext(tenantId, transformedRequest)));
  }

  private Observable<ResultSetChunk> executeTransformedRequest(
      QueryRequest transformedRequest, ExecutionContext context) {
    return Maybe.fromOptional(this.handlerSelector.select(transformedRequest, context))
        .switchIfEmpty(
            Maybe.error(
                Status.FAILED_PRECONDITION
                    .withDescription("No handler available matching request")
                    .asException()))
        .flatMapObservable(
            handler -> {
              handler.getTimeFilterColumn().ifPresent(context::setTimeFilterColumn);
              return handler.handleRequest(transformedRequest, context);
            })
        .lift(chunkRows(context.getResultSetMetadata()));
  }
}
