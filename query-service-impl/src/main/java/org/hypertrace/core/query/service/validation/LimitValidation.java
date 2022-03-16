package org.hypertrace.core.query.service.validation;

import io.grpc.Status;
import io.reactivex.rxjava3.core.Completable;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.query.service.QueryServiceConfig;
import org.hypertrace.core.query.service.QueryServiceConfig.LimitValidationConfig;
import org.hypertrace.core.query.service.api.QueryRequest;

@Slf4j
class LimitValidation implements QueryValidation {

  LimitValidationConfig config;

  @Inject
  LimitValidation(QueryServiceConfig queryServiceConfig) {
    this.config = queryServiceConfig.getLimitValidationConfig();
  }

  @Override
  public Completable validate(QueryRequest queryRequest, RequestContext requestContext) {

    switch (config.getMode()) {
      case ERROR:
        if (isInvalidLimit(queryRequest.getLimit())) {
          return Completable.error(
              Status.INVALID_ARGUMENT
                  .withDescription(generateErrorMessageForLimit(queryRequest.getLimit()))
                  .asException());
        }
        return Completable.complete();

      case WARN:
        if (isInvalidLimit(queryRequest.getLimit())) {
          log.warn(
              generateErrorMessageForLimit(queryRequest.getLimit())
                  + ". Allowing due to warn mode.{}{}",
              System.lineSeparator(),
              queryRequest);
        }
        return Completable.complete();
      case DISABLED:
      default:
        return Completable.complete();
    }
  }

  private String generateErrorMessageForLimit(int limit) {
    return String.format(
        "Received invalid query limit of %s, required to be in range of [%s, %s]",
        limit, config.getMin(), config.getMax());
  }

  private boolean isInvalidLimit(int limit) {
    return limit < config.getMin() || limit > config.getMax();
  }
}
