package org.hypertrace.core.query.service.prometheus;

import com.google.inject.Singleton;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
class PrometheusRestClientFactory {

  private final ConcurrentHashMap<String, PrometheusRestClient> clientMap =
      new ConcurrentHashMap<>();

  // Create a Prometheus Client.
  PrometheusRestClient createPrometheusClient(String requestHandlerName, String connectionString) {
    if (!containsClient(requestHandlerName)) {
      synchronized (PrometheusRestClientFactory.class) {
        if (!containsClient(requestHandlerName)) {
          addPrometheusClient(requestHandlerName, new PrometheusRestClient(connectionString));
        }
      }
    }
    return getPrometheusClient(requestHandlerName);
  }

  private void addPrometheusClient(String requestHandlerName, PrometheusRestClient client) {
    this.clientMap.put(requestHandlerName, client);
  }

  boolean containsClient(String requestHandlerName) {
    return this.clientMap.containsKey(requestHandlerName);
  }

  PrometheusRestClient getPrometheusClient(String requestHandlerName) {
    return this.clientMap.get(requestHandlerName);
  }
}
