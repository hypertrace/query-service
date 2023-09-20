package org.hypertrace.core.query.service.trino;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import org.hypertrace.core.query.service.RequestHandlerBuilder;
import org.hypertrace.core.query.service.RequestHandlerClientConfigRegistry;

public class TrinoModule extends AbstractModule {

  @Override
  protected void configure() {
    Multibinder.newSetBinder(binder(), RequestHandlerBuilder.class)
        .addBinding()
        .to(TrinoRequestHandlerBuilder.class);
    requireBinding(RequestHandlerClientConfigRegistry.class);
  }
}
