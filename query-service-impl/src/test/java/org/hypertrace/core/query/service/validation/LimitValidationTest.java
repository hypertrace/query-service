package org.hypertrace.core.query.service.validation;

import static org.hypertrace.core.query.service.QueryServiceConfig.LimitValidationConfig.LimitValidationMode.DISABLED;
import static org.hypertrace.core.query.service.QueryServiceConfig.LimitValidationConfig.LimitValidationMode.ERROR;
import static org.hypertrace.core.query.service.QueryServiceConfig.LimitValidationConfig.LimitValidationMode.WARN;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.StatusException;
import io.reactivex.rxjava3.observers.TestObserver;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.query.service.QueryServiceConfig;
import org.hypertrace.core.query.service.QueryServiceConfig.LimitValidationConfig.LimitValidationMode;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.junit.jupiter.api.Test;

class LimitValidationTest {
  RequestContext mockRequestContext = new RequestContext();

  @Test
  void doesNotValidateIfDisabled() {
    LimitValidation validation = new LimitValidation(buildConfig(1, 10, DISABLED));
    TestObserver<Void> observer = new TestObserver<>();
    validation
        .validate(QueryRequest.newBuilder().setLimit(-124).build(), mockRequestContext)
        .blockingSubscribe(observer);
    observer.assertComplete();
  }

  @Test
  void doesNotErrorOnInvalidLimitIfWarnMode() {
    LimitValidation validation = new LimitValidation(buildConfig(1, 10, WARN));
    TestObserver<Void> observer = new TestObserver<>();
    validation
        .validate(QueryRequest.newBuilder().setLimit(-124).build(), mockRequestContext)
        .blockingSubscribe(observer);
    observer.assertComplete();
  }

  @Test
  void allowsValidLimit() {
    LimitValidation validation = new LimitValidation(buildConfig(1, 10, ERROR));
    TestObserver<Void> observer = new TestObserver<>();
    validation
        .validate(QueryRequest.newBuilder().setLimit(5).build(), mockRequestContext)
        .blockingSubscribe(observer);
    observer.assertComplete();
  }

  @Test
  void errorsOnLimitMin() {
    LimitValidation validation = new LimitValidation(buildConfig(1, 10, ERROR));
    TestObserver<Void> observer = new TestObserver<>();
    validation
        .validate(QueryRequest.newBuilder().build(), mockRequestContext) // default 0
        .blockingSubscribe(observer);
    observer.assertError(StatusException.class);
  }

  @Test
  void errorsOnLimitMax() {
    LimitValidation validation = new LimitValidation(buildConfig(1, 10, ERROR));
    TestObserver<Void> observer = new TestObserver<>();
    validation
        .validate(QueryRequest.newBuilder().setLimit(15).build(), mockRequestContext)
        .blockingSubscribe(observer);
    observer.assertError(StatusException.class);
  }

  QueryServiceConfig buildConfig(int min, int max, LimitValidationMode mode) {
    QueryServiceConfig mockConfig = mock(QueryServiceConfig.class, RETURNS_DEEP_STUBS);
    when(mockConfig.getLimitValidationConfig().getMin()).thenReturn(min);
    when(mockConfig.getLimitValidationConfig().getMax()).thenReturn(max);
    when(mockConfig.getLimitValidationConfig().getMode()).thenReturn(mode);
    return mockConfig;
  }
}
