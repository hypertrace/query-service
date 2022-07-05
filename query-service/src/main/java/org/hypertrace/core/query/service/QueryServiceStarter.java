package org.hypertrace.core.query.service;

import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformServiceFactory;
import org.hypertrace.core.serviceframework.grpc.StandAloneGrpcPlatformServiceContainer;

public class QueryServiceStarter extends StandAloneGrpcPlatformServiceContainer {
  public QueryServiceStarter(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected GrpcPlatformServiceFactory getServiceFactory() {
    return new QueryServiceFactory();
  }
}
