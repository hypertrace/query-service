package org.hypertrace.core.query.service.multivalue;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import org.hypertrace.core.query.service.QueryTransformation;

public class MutliValueModule extends AbstractModule {
  @Override
  protected void configure() {
    Multibinder.newSetBinder(binder(), QueryTransformation.class)
        .addBinding()
        .to(MultiValueTransformation.class);
  }
}
