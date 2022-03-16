package org.hypertrace.core.query.service.validation;

import io.grpc.Status;
import io.grpc.StatusException;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.exceptions.CompositeException;
import io.reactivex.rxjava3.observers.TestObserver;
import java.util.Set;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.junit.jupiter.api.Test;

class QueryValidatorTest {
  QueryRequest mockRequest = QueryRequest.getDefaultInstance();
  RequestContext mockContext = new RequestContext();

  @Test
  void propagatesSingleError() {
    TestObserver<Void> observer = new TestObserver<>();
    QueryValidator validator =
        new QueryValidator(
            Set.of(new SuccessValidation(), new ErrorValidation(Status.UNKNOWN.asException())));
    validator.validate(mockRequest, mockContext).blockingSubscribe(observer);
    observer.assertError(StatusException.class);
  }

  @Test
  void propagatesMultipleErrors() {
    TestObserver<Void> observer = new TestObserver<>();
    QueryValidator validator =
        new QueryValidator(
            Set.of(
                new ErrorValidation(Status.DATA_LOSS.asException()),
                new ErrorValidation(Status.UNKNOWN.asException())));
    validator.validate(mockRequest, mockContext).blockingSubscribe(observer);
    observer.assertError(CompositeException.class);
  }

  @Test
  void propagatesSingleSuccess() {
    TestObserver<Void> observer = new TestObserver<>();
    QueryValidator validator = new QueryValidator(Set.of(new SuccessValidation()));
    validator.validate(mockRequest, mockContext).blockingSubscribe(observer);
    observer.assertComplete();
  }

  @Test
  void propagatesMultipleSuccess() {
    TestObserver<Void> observer = new TestObserver<>();
    QueryValidator validator =
        new QueryValidator(Set.of(new SuccessValidation(), new SuccessValidation()));
    validator.validate(mockRequest, mockContext).blockingSubscribe(observer);
    observer.assertComplete();
  }

  private static class ErrorValidation implements QueryValidation {
    Exception exception;

    ErrorValidation(Exception exception) {
      this.exception = exception;
    }

    @Override
    public Completable validate(QueryRequest queryRequest, RequestContext requestContext) {
      return Completable.error(exception);
    }
  }

  private static class SuccessValidation implements QueryValidation {
    @Override
    public Completable validate(QueryRequest queryRequest, RequestContext requestContext) {
      return Completable.complete();
    }
  }
}
