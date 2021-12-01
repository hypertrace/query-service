package org.hypertrace.core.query.service.prometheus;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import org.hypertrace.core.query.service.RequestHandlerBuilder;
import org.hypertrace.core.query.service.RequestHandlerClientConfigRegistry;
import org.hypertrace.core.query.service.pinot.PinotRequestHandlerBuilder;

public class PrometheusModule extends AbstractModule {

  @Override
  protected void configure() {
    Multibinder.newSetBinder(binder(), RequestHandlerBuilder.class)
        .addBinding()
        .to(PrometheusRequestHandlerBuilder.class);
    requireBinding(RequestHandlerClientConfigRegistry.class);
  }
}
