package org.hypertrace.core.query.service.pinot;

import javax.inject.Inject;
import org.hypertrace.core.query.service.QueryServiceImplConfig.ClientConfig;
import org.hypertrace.core.query.service.QueryServiceImplConfig.RequestHandlerConfig;
import org.hypertrace.core.query.service.RequestClientConfigRegistry;
import org.hypertrace.core.query.service.RequestHandler;
import org.hypertrace.core.query.service.RequestHandlerBuilder;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.Row;

public class PinotRequestHandlerBuilder implements RequestHandlerBuilder {

  private final RequestClientConfigRegistry clientConfigRegistry;

  @Inject
  PinotRequestHandlerBuilder(RequestClientConfigRegistry clientConfigRegistry) {
    this.clientConfigRegistry = clientConfigRegistry;
  }

  @Override
  public boolean canBuild(RequestHandlerConfig config) {
    return "pinot".equals(config.getType());
  }

  @Override
  public RequestHandler<QueryRequest, Row> build(RequestHandlerConfig config) {

    ClientConfig clientConfig =
        this.clientConfigRegistry
            .get(config.getClientConfig())
            .orElseThrow(
                () ->
                    new UnsupportedOperationException(
                        "Client config requested but not registered: " + config.getClientConfig()));

    PinotClientFactory.createPinotClient(
        config.getName(), clientConfig.getType(), clientConfig.getConnectionString());

    return new PinotBasedRequestHandler(config.getName(), config.getRequestHandlerInfo());
  }
}
