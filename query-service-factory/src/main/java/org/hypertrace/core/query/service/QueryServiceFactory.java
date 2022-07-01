package org.hypertrace.core.query.service;

import com.google.inject.Guice;
import java.util.List;
import org.hypertrace.core.query.service.api.QueryServiceGrpc.QueryServiceImplBase;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformService;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformServiceFactory;
import org.hypertrace.core.serviceframework.grpc.GrpcServiceContainerEnvironment;

public class QueryServiceFactory implements GrpcPlatformServiceFactory {
  private static final String SERVICE_NAME = "query-service";
  private static final String QUERY_SERVICE_CONFIG = "service.config";

  @Override
  public List<GrpcPlatformService> buildServices(GrpcServiceContainerEnvironment environment) {
    return List.of(
        new GrpcPlatformService(
            Guice.createInjector(
                    new QueryServiceModule(
                        environment.getConfig(SERVICE_NAME).getConfig(QUERY_SERVICE_CONFIG),
                        environment.getChannelRegistry()))
                .getInstance(QueryServiceImplBase.class)));
  }
}
