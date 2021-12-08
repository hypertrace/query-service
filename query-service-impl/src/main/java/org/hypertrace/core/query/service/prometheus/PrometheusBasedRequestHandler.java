package org.hypertrace.core.query.service.prometheus;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import io.reactivex.rxjava3.core.Observable;
import java.util.Optional;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.QueryCost;
import org.hypertrace.core.query.service.RequestHandler;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.Row;

public class PrometheusBasedRequestHandler implements RequestHandler {

  public static final String VIEW_DEFINITION_CONFIG_KEY = "prometheusViewDefinition";
  private static final String TENANT_COLUMN_NAME_CONFIG_KEY = "tenantAttributeName";
  private static final String START_TIME_ATTRIBUTE_NAME_CONFIG_KEY = "startTimeAttributeName";

  private final QueryRequestEligibilityValidator queryRequestEligibilityValidator;
  private final String name;
  private PrometheusViewDefinition prometheusViewDefinition;
  private Optional<String> startTimeAttributeName;
  private QueryRequestToPromqlConverter requestToPromqlConverter;

  PrometheusBasedRequestHandler(String name, Config config) {
    this.name = name;
    this.processConfig(config);
    this.queryRequestEligibilityValidator =
        new QueryRequestEligibilityValidator(prometheusViewDefinition);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Optional<String> getTimeFilterColumn() {
    return this.startTimeAttributeName;
  }

  private void processConfig(Config config) {

    if (!config.hasPath(TENANT_COLUMN_NAME_CONFIG_KEY)) {
      throw new RuntimeException(
          TENANT_COLUMN_NAME_CONFIG_KEY + " is not defined in the " + name + " request handler.");
    }

    String tenantColumnName = config.getString(TENANT_COLUMN_NAME_CONFIG_KEY);
    this.prometheusViewDefinition =
        PrometheusViewDefinition.parse(
            config.getConfig(VIEW_DEFINITION_CONFIG_KEY), tenantColumnName);

    this.startTimeAttributeName =
        config.hasPath(START_TIME_ATTRIBUTE_NAME_CONFIG_KEY)
            ? Optional.of(config.getString(START_TIME_ATTRIBUTE_NAME_CONFIG_KEY))
            : Optional.empty();

    this.requestToPromqlConverter = new QueryRequestToPromqlConverter(prometheusViewDefinition);
  }

  /**
   * Returns a QueryCost that is an indication of whether the given query can be handled by this
   * handler and if so, how costly is it to handle that query.
   */
  @Override
  public QueryCost canHandle(QueryRequest request, ExecutionContext executionContext) {
    return queryRequestEligibilityValidator.calculateCost(request, executionContext);
  }

  @Override
  public Observable<Row> handleRequest(
      QueryRequest originalRequest, ExecutionContext executionContext) {

    // Validate QueryContext and tenant id presence
    Preconditions.checkNotNull(executionContext);
    Preconditions.checkNotNull(executionContext.getTenantId());

    // todo call convert and execute request using client here

    return null;
  }
}
