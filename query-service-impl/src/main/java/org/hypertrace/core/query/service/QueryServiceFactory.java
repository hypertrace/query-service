package org.hypertrace.core.query.service;

import com.google.inject.Guice;
import com.typesafe.config.Config;
import org.hypertrace.core.query.service.api.QueryServiceGrpc.QueryServiceImplBase;

public class QueryServiceFactory {

  public static QueryServiceImplBase build(Config config) {
    return Guice.createInjector(new QueryServiceModule(config))
        .getInstance(QueryServiceImplBase.class);
  }
}
