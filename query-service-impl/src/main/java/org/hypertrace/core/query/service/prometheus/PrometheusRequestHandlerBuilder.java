package org.hypertrace.core.query.service.prometheus;

import javax.inject.Inject;
import org.hypertrace.core.query.service.QueryServiceConfig.RequestHandlerClientConfig;
import org.hypertrace.core.query.service.QueryServiceConfig.RequestHandlerConfig;
import org.hypertrace.core.query.service.RequestHandler;
import org.hypertrace.core.query.service.RequestHandlerBuilder;
import org.hypertrace.core.query.service.RequestHandlerClientConfigRegistry;

public class PrometheusRequestHandlerBuilder implements RequestHandlerBuilder {

  private final RequestHandlerClientConfigRegistry clientConfigRegistry;
  private final PrometheusRestClientFactory prometheusRestClientFactory;

  @Inject
  PrometheusRequestHandlerBuilder(
      RequestHandlerClientConfigRegistry clientConfigRegistry,
      PrometheusRestClientFactory prometheusRestClientFactory) {
    this.clientConfigRegistry = clientConfigRegistry;
    this.prometheusRestClientFactory = prometheusRestClientFactory;
  }

  @Override
  public boolean canBuild(RequestHandlerConfig config) {
    return "prometheus".equals(config.getType());
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

    return new PrometheusBasedRequestHandler(
        config.getName(),
        config.getRequestHandlerInfo(),
        prometheusRestClientFactory.getPrometheusClient(clientConfig.getConnectionString()));
  }
}
