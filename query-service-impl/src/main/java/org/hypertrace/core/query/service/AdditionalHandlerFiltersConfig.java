package org.hypertrace.core.query.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Operator;

@Slf4j
public class AdditionalHandlerFiltersConfig {
  private static final String ADDITIONAL_TENANT_FILTERS_CONFIG_KEY = "additionalTenantFilters";
  private final Map<String, List<Filter>> tenantToAdditionalFiltersMap;

  public AdditionalHandlerFiltersConfig(Config config, Optional<String> timeFilterColumnName) {
    if (config.hasPath(ADDITIONAL_TENANT_FILTERS_CONFIG_KEY)) {
      this.tenantToAdditionalFiltersMap =
          config.getConfigList(ADDITIONAL_TENANT_FILTERS_CONFIG_KEY).stream()
              .map(filterConfig -> new TenantFilters(filterConfig, timeFilterColumnName))
              .collect(
                  Collectors.toMap(
                      tenantFilters -> tenantFilters.tenantId,
                      tenantFilters -> tenantFilters.filters));
    } else {
      this.tenantToAdditionalFiltersMap = Collections.emptyMap();
    }
  }

  public List<Filter> getAdditionalFiltersForTenant(String tenantId) {
    return this.tenantToAdditionalFiltersMap.getOrDefault(tenantId, Collections.emptyList());
  }

  @lombok.Value
  @NonFinal
  private static class TenantFilters {
    private static final String TENANT_ID_CONFIG_KEY = "tenantId";
    private static final String TIME_RANGE_AND_FILTERS_CONFIG_KEY = "timeRangeAndFilters";
    String tenantId;
    List<Filter> filters;

    private TenantFilters(Config config, Optional<String> startTimeAttributeName) {
      this.tenantId = config.getString(TENANT_ID_CONFIG_KEY);
      this.filters =
          config.getConfigList(TIME_RANGE_AND_FILTERS_CONFIG_KEY).stream()
              .map(TimeRangeAndFilter::new)
              .map(
                  timeRangeAndFilter ->
                      timeRangeAndFilter.buildTimeRangeAndFilters(startTimeAttributeName))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .collect(Collectors.toList());
    }
  }

  @lombok.Value
  @NonFinal
  private static class TimeRangeAndFilter {
    private static final String START_TIME_CONFIG_PATH = "startTimeMillis";
    private static final String END_TIME_CONFIG_PATH = "endTimeMillis";
    private static final String FILTER_CONFIG_PATH = "filter";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    Optional<Long> startTimeMillis;
    Optional<Long> endTimeMillis;
    Optional<Filter> filter;

    private TimeRangeAndFilter(Config config) {
      if (config.hasPath(START_TIME_CONFIG_PATH) && config.hasPath(END_TIME_CONFIG_PATH)) {
        this.startTimeMillis = Optional.of(config.getLong(START_TIME_CONFIG_PATH));
        this.endTimeMillis = Optional.of(config.getLong(END_TIME_CONFIG_PATH));
      } else {
        startTimeMillis = Optional.empty();
        endTimeMillis = Optional.empty();
      }
      if (config.hasPath(FILTER_CONFIG_PATH)) {
        this.filter = deserializeFilter(config.getString(FILTER_CONFIG_PATH));
      } else {
        filter = Optional.empty();
      }
    }

    private Optional<Filter> buildTimeRangeAndFilters(Optional<String> timeRangeAttribute) {
      Filter.Builder filterBuilder = Filter.newBuilder();
      filterBuilder.setOperator(Operator.OR);
      if (timeRangeAttribute.isPresent()) {
        if (this.startTimeMillis.isPresent() && this.endTimeMillis.isPresent()) {
          filterBuilder.addChildFilter(
              QueryRequestUtil.createFilter(
                  timeRangeAttribute.get(), Operator.GT, this.endTimeMillis.get()));
          filterBuilder.addChildFilter(
              QueryRequestUtil.createFilter(
                  timeRangeAttribute.get(), Operator.LT, this.startTimeMillis.get()));
        }
      }

      this.filter.ifPresent(filterBuilder::addChildFilter);
      if (filterBuilder.getChildFilterCount() == 0) {
        return Optional.empty();
      }
      if (filterBuilder.getChildFilterCount() == 1) {
        return Optional.of(filterBuilder.getChildFilter(0));
      }

      return Optional.of(filterBuilder.build());
    }

    private Optional<Filter> deserializeFilter(String filterJson) {
      try {
        JsonNode jsonNode = objectMapper.readTree(filterJson);

        Filter.Builder filterBuilder = Filter.newBuilder();
        JsonFormat.parser().merge(jsonNode.toString(), filterBuilder);

        return Optional.of(filterBuilder.build());

      } catch (Exception e) {
        log.error("Error deserializing additional filter config to query request filter");
        return Optional.empty();
      }
    }
  }
}
