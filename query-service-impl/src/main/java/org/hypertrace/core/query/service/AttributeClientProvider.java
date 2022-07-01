package org.hypertrace.core.query.service;

import static org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider;

import io.grpc.Channel;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import javax.inject.Inject;
import javax.inject.Provider;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

final class AttributeClientProvider implements Provider<CachingAttributeClient> {

  private final QueryServiceConfig config;
  private final GrpcChannelRegistry grpcChannelRegistry;

  @Inject
  AttributeClientProvider(QueryServiceConfig config, GrpcChannelRegistry grpcChannelRegistry) {
    this.config = config;
    this.grpcChannelRegistry = grpcChannelRegistry;
  }

  @Override
  public CachingAttributeClient get() {
    Channel channel =
        grpcChannelRegistry.forPlaintextAddress(
            config.getAttributeClientConfig().getHost(),
            config.getAttributeClientConfig().getPort());

    return CachingAttributeClient.builder(channel)
        .withCallCredentials(getClientCallCredsProvider().get())
        .withCacheExpiration(Duration.of(15, ChronoUnit.MINUTES))
        .build();
  }
}
