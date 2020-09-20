package org.hypertrace.core.query.service;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.hypertrace.core.query.service.QueryServiceImplConfig.ClientConfig;

public class RequestClientConfigRegistry {
  private final Map<String, ClientConfig> clientConfigMap;

  @Inject
  RequestClientConfigRegistry(QueryServiceImplConfig queryServiceImplConfig) {
    this.clientConfigMap =
        queryServiceImplConfig.getClients().stream()
            .map(ClientConfig::parse)
            .collect(Collectors.toUnmodifiableMap(ClientConfig::getType, Function.identity()));
  }

  public Optional<ClientConfig> get(String key) {
    return Optional.ofNullable(this.clientConfigMap.get(key));
  }
}
