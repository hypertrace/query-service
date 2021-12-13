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
      String configName, String connectionString) {
    if (!get().containsClient(configName)) {
      synchronized (get()) {
        if (!get().containsClient(configName)) {
          get().addPrometheusClient(configName, new PrometheusRestClient(connectionString));
        }
      }
    }
    return get().getPrometheusClient(configName);
  }

  public static PrometheusRestClientFactory get() {
    return INSTANCE;
  }

  private void addPrometheusClient(String cluster, PrometheusRestClient client) {
    this.clientMap.put(cluster, client);
  }

  public boolean containsClient(String configName) {
    return this.clientMap.containsKey(configName);
  }

  public PrometheusRestClient getPrometheusClient(String configName) {
    return this.clientMap.get(configName);
  }
}
