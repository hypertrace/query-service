package org.hypertrace.core.query.service;

import static org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import javax.inject.Inject;
import javax.inject.Provider;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;

final class AttributeClientProvider implements Provider<CachingAttributeClient> {

  private final QueryServiceConfig config;

  @Inject
  AttributeClientProvider(QueryServiceConfig config) {
    this.config = config;
  }

  @Override
  public CachingAttributeClient get() {
    return CachingAttributeClient.builder()
        .withNewChannel(
            config.getAttributeClientConfig().getHost(),
            config.getAttributeClientConfig().getPort())
        .withCallCredentials(getClientCallCredsProvider().get())
        .withCacheExpiration(Duration.of(15, ChronoUnit.MINUTES))
        .build();
  }
}
