package org.hypertrace.core.query.service;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import com.typesafe.config.Config;
import javax.inject.Singleton;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.query.service.api.QueryServiceGrpc.QueryServiceImplBase;
import org.hypertrace.core.query.service.attribubteexpression.AttributeExpressionModule;
import org.hypertrace.core.query.service.multivalue.MutliValueModule;
import org.hypertrace.core.query.service.pinot.PinotModule;
import org.hypertrace.core.query.service.postgres.PostgresModule;
import org.hypertrace.core.query.service.projection.ProjectionModule;
import org.hypertrace.core.query.service.prometheus.PrometheusModule;
import org.hypertrace.core.query.service.trino.TrinoModule;
import org.hypertrace.core.query.service.validation.QueryValidationModule;

class QueryServiceModule extends AbstractModule {

  private final QueryServiceConfig config;
  private final GrpcChannelRegistry grpcChannelRegistry;

  QueryServiceModule(Config config, GrpcChannelRegistry grpcChannelRegistry) {
    this.config = new QueryServiceConfig(config);
    this.grpcChannelRegistry = grpcChannelRegistry;
  }

  @Override
  protected void configure() {
    bind(QueryServiceConfig.class).toInstance(this.config);
    bind(GrpcChannelRegistry.class).toInstance(this.grpcChannelRegistry);
    bind(QueryServiceImplBase.class).to(QueryServiceImpl.class);
    Multibinder.newSetBinder(binder(), QueryTransformation.class);
    bind(CachingAttributeClient.class)
        .toProvider(AttributeClientProvider.class)
        .in(Singleton.class);
    install(new PinotModule());
    install(new PostgresModule());
    install(new TrinoModule());
    install(new ProjectionModule());
    install(new MutliValueModule());
    install(new PrometheusModule());
    install(new AttributeExpressionModule());
    install(new QueryValidationModule());
  }
}
