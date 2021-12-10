package org.hypertrace.core.query.service.prometheus;

import static java.util.Objects.requireNonNull;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

class PrometheusTestUtils {

  private static final String TENANT_COLUMN_NAME = "tenant_id";
  private static final String TEST_REQUEST_HANDLER_CONFIG_FILE = "prometheus_request_handler.conf";

  static PrometheusViewDefinition getDefaultPrometheusViewDefinition() {
    Config fileConfig = getDefaultPrometheusConfig();
    return PrometheusViewDefinition.parse(
        fileConfig.getConfig("requestHandlerInfo.prometheusViewDefinition"), TENANT_COLUMN_NAME);
  }

  static Config getDefaultPrometheusConfig() {
    return ConfigFactory.parseURL(
        requireNonNull(
            QueryRequestToPromqlConverterTest.class
                .getClassLoader()
                .getResource(TEST_REQUEST_HANDLER_CONFIG_FILE)));
  }
}
