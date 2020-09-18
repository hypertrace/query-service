package org.hypertrace.core.query.service;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import java.util.List;

public class QueryServiceImplConfig {
  private List<Config> clients;
  private List<Config> queryRequestHandlersConfig;

  static QueryServiceImplConfig parse(Config config) {
    return ConfigBeanFactory.create(config, QueryServiceImplConfig.class);
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

  public static class RequestHandlerConfig {

    private String name;
    private String type;
    private String clientConfig;
    private Config requestHandlerInfo;

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

    public Config getRequestHandlerInfo() {
      return requestHandlerInfo;
    }

    public void setRequestHandlerInfo(Config requestHandlerInfo) {
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
