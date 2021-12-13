package org.hypertrace.core.query.service.prometheus;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import io.reactivex.rxjava3.core.Observable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import okhttp3.Request;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.QueryCost;
import org.hypertrace.core.query.service.QueryRequestUtil;
import org.hypertrace.core.query.service.RequestHandler;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Expression.ValueCase;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.Row;
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

  PrometheusBasedRequestHandler(String name, Config requestHandlerConfig) {
    this.name = name;
    this.processConfig(requestHandlerConfig);
    this.queryRequestEligibilityValidator =
        new QueryRequestEligibilityValidator(prometheusViewDefinition);
    this.requestToPromqlConverter = new QueryRequestToPromqlConverter(prometheusViewDefinition);
    this.prometheusRestClient = PrometheusRestClientFactory.get().getPrometheusClient(name);
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

    Map<Request, PromQLMetricResponse> responseMap;
    Map<String, String> logicalAttributeNameToMetricQueryMap = new LinkedHashMap<>();
    if (isRangeQueryRequest(originalRequest)) {
      PromQLRangeQueries promQLRangeQueries =
          requestToPromqlConverter.convertToPromqlRangeQuery(
              executionContext,
              originalRequest,
              executionContext.getAllSelections(),
              logicalAttributeNameToMetricQueryMap);
      responseMap = prometheusRestClient.executeRangeQuery(promQLRangeQueries);
    } else {
      PromQLInstantQueries promQLInstantQueries =
          requestToPromqlConverter.convertToPromqlInstantQuery(
              executionContext,
              originalRequest,
              executionContext.getAllSelections(),
              logicalAttributeNameToMetricQueryMap);
      responseMap = prometheusRestClient.executeInstantQuery(promQLInstantQueries);
    }

    List<Row> rows =
        PrometheusBasedResponseBuilder.buildResponse(
            responseMap,
            prometheusViewDefinition.getAttributeMap(),
            logicalAttributeNameToMetricQueryMap,
            prepareSelectionColumnSet(executionContext.getAllSelections(), executionContext),
            executionContext.getTimeFilterColumn());

    return Observable.fromIterable(rows).doOnNext(row -> LOG.debug("collect a row: {}", row));
  }

  private boolean isRangeQueryRequest(QueryRequest queryRequest) {
    return queryRequest.getGroupByList().stream().anyMatch(QueryRequestUtil::isDateTimeFunction);
  }

  private LinkedHashSet<String> prepareSelectionColumnSet(
      LinkedHashSet<Expression> expressions, ExecutionContext executionContext) {
    LinkedHashSet<String> selectionColumnSet = new LinkedHashSet<>();

    expressions.forEach(
        expression -> {
          ValueCase valueCase = expression.getValueCase();
          switch (valueCase) {
            case ATTRIBUTE_EXPRESSION:
            case COLUMNIDENTIFIER:
              selectionColumnSet.add(QueryRequestUtil.getLogicalColumnName(expression));
              break;
            case FUNCTION:
              if (QueryRequestUtil.isDateTimeFunction(expression)) {
                selectionColumnSet.add(executionContext.getTimeFilterColumn());
              } else {
                selectionColumnSet.add(PrometheusUtils.getColumnNameForMetricFunction(expression));
              }
              break;
            default:
              throw new IllegalArgumentException("un-supported selection for promql request");
          }
        });

    return selectionColumnSet;
  }
}
