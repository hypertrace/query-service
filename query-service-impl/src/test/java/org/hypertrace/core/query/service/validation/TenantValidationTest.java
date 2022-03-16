package org.hypertrace.core.query.service.validation;

import io.grpc.StatusException;
import io.reactivex.rxjava3.observers.TestObserver;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.junit.jupiter.api.Test;

class TenantValidationTest {
  QueryRequest mockRequest = QueryRequest.getDefaultInstance();
  TenantValidation tenantValidation = new TenantValidation();

  @Test
  void errorsOnMissingTenant() {
    TestObserver<Void> observer = new TestObserver<>();
    tenantValidation.validate(mockRequest, new RequestContext()).blockingSubscribe(observer);
    observer.assertError(StatusException.class);
  }

  @Test
  void errorsOnEmptyTenant() {
    TestObserver<Void> observer = new TestObserver<>();
    tenantValidation
        .validate(mockRequest, RequestContext.forTenantId(""))
        .blockingSubscribe(observer);
    observer.assertError(StatusException.class);
  }

  @Test
  void passesOnValidTenant() {
    TestObserver<Void> observer = new TestObserver<>();
    tenantValidation
        .validate(mockRequest, RequestContext.forTenantId("good-id"))
        .blockingSubscribe(observer);
    observer.assertComplete();
  }
}
