package org.hypertrace.core.query.service.validation;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Observable;
import java.util.Set;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.query.service.api.QueryRequest;

/**
 * Query validator invokes each registered validation, passing only if all complete succesfully.
 * Validations may be performed in any order. Any failing validation is responsible for producing
 * its own error which will be passed to the caller.
 */
public class QueryValidator {
  private final Set<QueryValidation> validations;

  @Inject
  QueryValidator(Set<QueryValidation> validations) {
    this.validations = validations;
  }

  public Completable validate(QueryRequest originalRequest, RequestContext requestContext) {
    return Observable.fromIterable(validations)
        .flatMapCompletable(
            queryValidation -> queryValidation.validate(originalRequest, requestContext), true);
  }
}
