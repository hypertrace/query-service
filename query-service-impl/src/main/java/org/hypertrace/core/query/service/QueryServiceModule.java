package org.hypertrace.core.query.service;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import com.typesafe.config.Config;
import org.hypertrace.core.query.service.api.QueryServiceGrpc.QueryServiceImplBase;
import org.hypertrace.core.query.service.pinot.PinotModule;

class QueryServiceModule extends AbstractModule {

  private final QueryServiceImplConfig config;

  QueryServiceModule(Config config) {
    this.config = QueryServiceImplConfig.parse(config);
  }

  @Override
  protected void configure() {
    bind(QueryServiceImplConfig.class).toInstance(this.config);
    bind(QueryServiceImplBase.class).to(QueryServiceImpl.class);
    Multibinder.newSetBinder(binder(), QueryTransformation.class);
    install(new PinotModule());
  }
}
