package org.hypertrace.core.query.service.prometheus;

import com.google.inject.Singleton;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
class PrometheusRestClientFactory {

  private final ConcurrentHashMap<String, PrometheusRestClient> clientMap =
      new ConcurrentHashMap<>();

  PrometheusRestClient getPrometheusClient(String connectionString) {
    return this.clientMap.computeIfAbsent(connectionString, PrometheusRestClient::new);
  }
}
