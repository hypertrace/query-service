package org.hypertrace.core.query.service.postgres;

import java.sql.SQLException;
import javax.inject.Inject;
import org.hypertrace.core.query.service.QueryServiceConfig.RequestHandlerClientConfig;
import org.hypertrace.core.query.service.QueryServiceConfig.RequestHandlerConfig;
import org.hypertrace.core.query.service.RequestHandler;
import org.hypertrace.core.query.service.RequestHandlerBuilder;
import org.hypertrace.core.query.service.RequestHandlerClientConfigRegistry;

public class PostgresRequestHandlerBuilder implements RequestHandlerBuilder {

  private final RequestHandlerClientConfigRegistry clientConfigRegistry;

  @Inject
  PostgresRequestHandlerBuilder(RequestHandlerClientConfigRegistry clientConfigRegistry) {
    this.clientConfigRegistry = clientConfigRegistry;
  }

  @Override
  public boolean canBuild(RequestHandlerConfig config) {
    return "postgres".equals(config.getType());
  }

  @Override
  public RequestHandler build(RequestHandlerConfig config) {

    RequestHandlerClientConfig clientConfig =
        this.clientConfigRegistry
            .get(config.getClientConfig())
            .orElseThrow(
                () ->
                    new UnsupportedOperationException(
                        "Client config requested but not registered: " + config.getClientConfig()));

    try {
      PostgresClientFactory.createPostgresClient(config.getName(), clientConfig);
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }

    return new PostgresBasedRequestHandler(config.getName(), config.getRequestHandlerInfo());
  }
}
