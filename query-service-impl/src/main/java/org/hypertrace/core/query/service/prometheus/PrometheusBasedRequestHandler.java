package org.hypertrace.core.query.service.prometheus;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import io.reactivex.rxjava3.core.Observable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import okhttp3.Request;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.QueryCost;
import org.hypertrace.core.query.service.QueryRequestUtil;
import org.hypertrace.core.query.service.RequestHandler;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.Row.Builder;
import org.hypertrace.core.query.service.pinot.PinotBasedRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrometheusBasedRequestHandler implements RequestHandler {

  private static final Logger LOG = LoggerFactory.getLogger(PrometheusBasedRequestHandler.class);

  private static final String VIEW_DEFINITION_CONFIG_KEY = "prometheusViewDefinition";
  private static final String TENANT_ATTRIBUTE_NAME_CONFIG_KEY = "tenantAttributeName";
  private static final String START_TIME_ATTRIBUTE_NAME_CONFIG_KEY = "startTimeAttributeName";

  private final QueryRequestEligibilityValidator queryRequestEligibilityValidator;
  private final String name;
  private final QueryRequestToPromqlConverter requestToPromqlConverter;
  private final PrometheusRestClient prometheusRestClient;

  private Optional<String> startTimeAttributeName;
  private PrometheusViewDefinition prometheusViewDefinition;

  PrometheusBasedRequestHandler(String name, Config config) {
    this.name = name;
    this.processConfig(config);
    this.queryRequestEligibilityValidator =
        new QueryRequestEligibilityValidator(prometheusViewDefinition);
    this.requestToPromqlConverter = new QueryRequestToPromqlConverter(prometheusViewDefinition);
    this.prometheusRestClient = null;
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

    if (!config.hasPath(TENANT_ATTRIBUTE_NAME_CONFIG_KEY)) {
      throw new RuntimeException(
          TENANT_ATTRIBUTE_NAME_CONFIG_KEY
              + " is not defined in the "
              + name
              + " request handler.");
    }

    String tenantAttributeName = config.getString(TENANT_ATTRIBUTE_NAME_CONFIG_KEY);
    this.prometheusViewDefinition =
        PrometheusViewDefinition.parse(
            config.getConfig(VIEW_DEFINITION_CONFIG_KEY), tenantAttributeName);

    this.startTimeAttributeName =
        config.hasPath(START_TIME_ATTRIBUTE_NAME_CONFIG_KEY)
            ? Optional.of(config.getString(START_TIME_ATTRIBUTE_NAME_CONFIG_KEY))
            : Optional.empty();
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

    Map<Request, PromQLMetricResponse> responseMap;
    Map<String, String> metricNameToQueryMap;
    if (isRangeQueryRequest(originalRequest)) {
      PromQLRangeQueries promQLRangeQueries =
          requestToPromqlConverter.convertToPromqlRangeQuery(
              executionContext, originalRequest, executionContext.getAllSelections());
      metricNameToQueryMap = promQLRangeQueries.getMetricNameToQueryMap();
      responseMap = prometheusRestClient.executeRangeQuery(promQLRangeQueries);
    } else {
      PromQLInstantQueries promQLInstantQueries =
          requestToPromqlConverter.convertToPromqlInstantQuery(
              executionContext, originalRequest, executionContext.getAllSelections());
      metricNameToQueryMap = promQLInstantQueries.getMetricNameToQueryMap();
      responseMap = prometheusRestClient.executeInstantQuery(promQLInstantQueries);
    }

    List<Row> rows =
        PrometheusBasedResponseBuilder.buildResponse(
            responseMap,
            prometheusViewDefinition.getAttributeMap(),
            metricNameToQueryMap,
            executionContext.getColumnSet(),
            executionContext.getTimeFilterColumn());

    return Observable.fromIterable(rows)
        .doOnNext(row -> LOG.debug("collect a row: {}", row));
  }

  private boolean isRangeQueryRequest(QueryRequest queryRequest) {
    return queryRequest.getGroupByList().stream().anyMatch(QueryRequestUtil::isDateTimeFunction);
  }
}
