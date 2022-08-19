package org.hypertrace.core.query.service;

import com.typesafe.config.Config;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Value;
import lombok.experimental.NonFinal;

@Value
@NonFinal
public class QueryServiceConfig {

  private static final String CONFIG_PATH_HANDLER_CLIENT_LIST = "clients";
  private static final String CONFIG_PATH_HANDLER_CONFIG_LIST = "queryRequestHandlersConfig";
  private static final String CONFIG_PATH_ATTRIBUTE_CLIENT = "attribute.client";
  private static final String CONFIG_PATH_LIMIT_VALIDATION = "validation.limit";

  List<RequestHandlerClientConfig> requestHandlerClientConfigs;
  List<RequestHandlerConfig> queryRequestHandlersConfigs;
  ClientHostPortConfig attributeClientConfig;
  LimitValidationConfig limitValidationConfig;

  QueryServiceConfig(Config config) {
    Config resolved = config.resolve();
    this.attributeClientConfig =
        new ClientHostPortConfig(resolved.getConfig(CONFIG_PATH_ATTRIBUTE_CLIENT));
    this.requestHandlerClientConfigs =
        resolved.getConfigList(CONFIG_PATH_HANDLER_CLIENT_LIST).stream()
            .map(RequestHandlerClientConfig::new)
            .collect(Collectors.toUnmodifiableList());
    this.queryRequestHandlersConfigs =
        resolved.getConfigList(CONFIG_PATH_HANDLER_CONFIG_LIST).stream()
            .map(RequestHandlerConfig::new)
            .collect(Collectors.toUnmodifiableList());
    this.limitValidationConfig =
        new LimitValidationConfig(resolved.getConfig(CONFIG_PATH_LIMIT_VALIDATION));
  }

  @Value
  @NonFinal
  public static class RequestHandlerConfig {
    private static final String CONFIG_PATH_NAME = "name";
    private static final String CONFIG_PATH_TYPE = "type";
    private static final String CONFIG_PATH_CLIENT_KEY = "clientConfig";
    private static final String CONFIG_PATH_REQUEST_HANDLER_INFO = "requestHandlerInfo";

    String name;
    String type;
    String clientConfig;
    Config requestHandlerInfo;

    private RequestHandlerConfig(Config config) {
      this.name = config.getString(CONFIG_PATH_NAME);
      this.type = config.getString(CONFIG_PATH_TYPE);
      this.clientConfig = config.getString(CONFIG_PATH_CLIENT_KEY);
      this.requestHandlerInfo = config.getConfig(CONFIG_PATH_REQUEST_HANDLER_INFO);
    }
  }

  @Value
  @NonFinal
  public static class RequestHandlerClientConfig {
    private static final String CONFIG_PATH_TYPE = "type";
    private static final String CONFIG_PATH_CONNECTION_STRING = "connectionString";
    private static final String CONFIG_PATH_USER = "user";
    private static final String CONFIG_PATH_PASSWORD = "password";
    private static final String CONFIG_PATH_MAX_CONNECTIONS = "maxConnections";
    String type;
    String connectionString;
    Optional<String> user;
    Optional<String> password;
    Optional<Integer> maxConnections;

    private RequestHandlerClientConfig(Config config) {
      this.type = config.getString(CONFIG_PATH_TYPE);
      this.connectionString = config.getString(CONFIG_PATH_CONNECTION_STRING);
      this.user =
          config.hasPath(CONFIG_PATH_USER)
              ? Optional.of(config.getString(CONFIG_PATH_USER))
              : Optional.empty();
      this.password =
          config.hasPath(CONFIG_PATH_PASSWORD)
              ? Optional.of(config.getString(CONFIG_PATH_PASSWORD))
              : Optional.empty();
      this.maxConnections =
          config.hasPath(CONFIG_PATH_MAX_CONNECTIONS)
              ? Optional.of(config.getInt(CONFIG_PATH_MAX_CONNECTIONS))
              : Optional.empty();
    }
  }

  @Value
  @NonFinal
  public static class ClientHostPortConfig {
    private static final String CONFIG_PATH_HOST = "host";
    private static final String CONFIG_PATH_PORT = "port";
    String host;
    int port;

    private ClientHostPortConfig(Config config) {
      this.host = config.getString(CONFIG_PATH_HOST);
      this.port = config.getInt(CONFIG_PATH_PORT);
    }
  }

  @Value
  @NonFinal
  public static class LimitValidationConfig {
    private static final String CONFIG_PATH_MIN = "min";
    private static final String CONFIG_PATH_MAX = "max";
    private static final String CONFIG_PATH_MODE = "mode";
    int min;
    int max;
    LimitValidationMode mode;

    private LimitValidationConfig(Config config) {
      this.min = config.getInt(CONFIG_PATH_MIN);
      this.max = config.getInt(CONFIG_PATH_MAX);
      this.mode = config.getEnum(LimitValidationMode.class, CONFIG_PATH_MODE);
    }

    public enum LimitValidationMode {
      DISABLED,
      WARN,
      ERROR
    }
  }
}
