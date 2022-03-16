package org.hypertrace.core.query.service.validation;

import io.reactivex.rxjava3.core.Completable;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.query.service.api.QueryRequest;

public interface QueryValidation {
  Completable validate(QueryRequest queryRequest, RequestContext requestContext);
}
