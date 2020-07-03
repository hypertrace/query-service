package org.hypertrace.core.query.service;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import java.util.List;
import java.util.Map;

public class QueryServiceImplConfig {
  private String tenantColumnName;
  private List<Config> clients;
  private List<Config> queryRequestHandlersConfig;

  public static QueryServiceImplConfig parse(Config config) {
    return ConfigBeanFactory.create(config, QueryServiceImplConfig.class);
  }

  public String getTenantColumnName() {
    return tenantColumnName;
  }

  public List<Config> getClients() {
    return this.clients;
  }

  public void setClients(List<Config> clients) {
    this.clients = clients;
  }

  public List<Config> getQueryRequestHandlersConfig() {
    return this.queryRequestHandlersConfig;
  }

  public void setQueryRequestHandlersConfig(List<Config> queryRequestHandlersConfig) {
    this.queryRequestHandlersConfig = queryRequestHandlersConfig;
  }

  public void setTenantColumnName(String tenantColumnName) {
    this.tenantColumnName = tenantColumnName;
  }

  public static class RequestHandlerConfig {

    private String name;
    private String type;
    private String clientConfig;
    private Map<String, Object> requestHandlerInfo;

    public static RequestHandlerConfig parse(Config config) {
      return ConfigBeanFactory.create(config, RequestHandlerConfig.class);
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public String getClientConfig() {
      return clientConfig;
    }

    public void setClientConfig(String clientConfig) {
      this.clientConfig = clientConfig;
    }

    public Map<String, Object> getRequestHandlerInfo() {
      return requestHandlerInfo;
    }

    public void setRequestHandlerInfo(Map<String, Object> requestHandlerInfo) {
      this.requestHandlerInfo = requestHandlerInfo;
    }
  }

  public static class ClientConfig {

    private String type;
    private String connectionString;

    public static ClientConfig parse(Config config) {
      return ConfigBeanFactory.create(config, ClientConfig.class);
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public String getConnectionString() {
      return connectionString;
    }

    public void setConnectionString(String connectionString) {
      this.connectionString = connectionString;
    }
  }
}
