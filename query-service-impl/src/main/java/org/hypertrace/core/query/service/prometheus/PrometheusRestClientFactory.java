package org.hypertrace.core.query.service.prometheus;

import java.util.concurrent.ConcurrentHashMap;

class PrometheusRestClientFactory {

  // Singleton instance
  private static final PrometheusRestClientFactory INSTANCE = new PrometheusRestClientFactory();

  private final ConcurrentHashMap<String, PrometheusRestClient> clientMap =
      new ConcurrentHashMap<>();

  private PrometheusRestClientFactory() {}

  // Create a Prometheus Client.
  public static PrometheusRestClient createPrometheusClient(
      String requestHandlerName, String connectionString) {
    if (!get().containsClient(requestHandlerName)) {
      synchronized (get()) {
        if (!get().containsClient(requestHandlerName)) {
          get().addPrometheusClient(requestHandlerName, new PrometheusRestClient(connectionString));
        }
      }
    }
    return get().getPrometheusClient(requestHandlerName);
  }

  public static PrometheusRestClientFactory get() {
    return INSTANCE;
  }

  private void addPrometheusClient(String requestHandlerName, PrometheusRestClient client) {
    this.clientMap.put(requestHandlerName, client);
  }

  public boolean containsClient(String requestHandlerName) {
    return this.clientMap.containsKey(requestHandlerName);
  }

  public PrometheusRestClient getPrometheusClient(String requestHandlerName) {
    return this.clientMap.get(requestHandlerName);
  }
}
