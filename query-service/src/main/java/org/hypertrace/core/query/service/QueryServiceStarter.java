package org.hypertrace.core.query.service;

import java.util.List;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformServerDefinition;
import org.hypertrace.core.serviceframework.grpc.StandAloneGrpcPlatformServiceContainer;

public class QueryServiceStarter extends StandAloneGrpcPlatformServiceContainer {
  public QueryServiceStarter(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected List<GrpcPlatformServerDefinition> getServerDefinitions() {
    return List.of(
        GrpcPlatformServerDefinition.builder()
            .name(this.getServiceName())
            .port(this.getServicePort())
            .serviceFactory(new QueryServiceFactory())
            .build());
  }
}
