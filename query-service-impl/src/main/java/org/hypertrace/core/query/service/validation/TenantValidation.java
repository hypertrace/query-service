package org.hypertrace.core.query.service.validation;

import io.grpc.Status;
import io.reactivex.rxjava3.core.Completable;
import java.util.function.Predicate;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.query.service.api.QueryRequest;

class TenantValidation implements QueryValidation {
  @Override
  public Completable validate(QueryRequest queryRequest, RequestContext requestContext) {
    if (requestContext.getTenantId().filter(Predicate.not(String::isEmpty)).isEmpty()) {
      return Completable.error(
          Status.INVALID_ARGUMENT.withDescription("Tenant ID is missing on request").asException());
    }
    return Completable.complete();
  }
}
