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
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.QueryServiceGrpc;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.validation.QueryValidator;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Singleton
@Slf4j
class QueryServiceImpl extends QueryServiceGrpc.QueryServiceImplBase {

  private final RequestHandlerSelector handlerSelector;
  private final QueryTransformationPipeline queryTransformationPipeline;
  private final QueryValidator queryValidator;

  private Counter requestStatusErrorCounter;
  private Counter requestStatusSuccessCounter;
  private static final String SERVICE_REQUESTS_STATUS_COUNTER =
      "hypertrace.query.service.requests.status";

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

  private void initMetrics() {
    requestStatusErrorCounter =
        PlatformMetricsRegistry.registerCounter(
            SERVICE_REQUESTS_STATUS_COUNTER, ImmutableMap.of("error", "true"));

    requestStatusSuccessCounter =
        PlatformMetricsRegistry.registerCounter(
            SERVICE_REQUESTS_STATUS_COUNTER, ImmutableMap.of("error", "false"));
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
        .doOnError(
            error -> {
              log.error("Query failed: {}", originalRequest, error);
              requestStatusErrorCounter.increment();
            })
        .doOnComplete(() -> requestStatusSuccessCounter.increment())
        .subscribe(
            new ServerCallStreamRxObserver<>(
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
              return handler.handleRequest(
                  handler.applyAdditionalFilters(transformedRequest, context), context);
            })
        .lift(chunkRows(context.getResultSetMetadata()));
  }

  public static void main(String[] args) {
    ColumnIdentifier serviceNameColumn =
        ColumnIdentifier.newBuilder().setColumnName("SERVICE.name").build();
    Expression lhs = Expression.newBuilder().setColumnIdentifier(serviceNameColumn).build();

    LiteralConstant constant =
        LiteralConstant.newBuilder()
            .setValue(Value.newBuilder().setString(String.valueOf("dummyService")).build())
            .build();
    Expression rhs = Expression.newBuilder().setLiteral(constant).build();
    Filter filter = Filter.newBuilder().setLhs(lhs).setRhs(rhs).setOperator(Operator.EQ).build();
    System.out.println(filter);
  }
}
