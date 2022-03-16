package org.hypertrace.core.query.service.validation;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;

public class QueryValidationModule extends AbstractModule {
  @Override
  protected void configure() {
    Multibinder<QueryValidation> validationMultibinder =
        Multibinder.newSetBinder(binder(), QueryValidation.class);
    validationMultibinder.addBinding().to(TenantValidation.class);
    validationMultibinder.addBinding().to(LimitValidation.class);
  }
}
