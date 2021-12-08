package org.hypertrace.core.query.service.prometheus;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigUtil;
import com.typesafe.config.ConfigValue;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Getter;

/** Prometheus metric & attribute mapping for a pinot view */
class PrometheusViewDefinition {

  private static final String VIEW_NAME_CONFIG_KEY = "viewName";
  private static final String ATTRIBUTE_MAP_CONFIG_KEY = "attributeMap";
  private static final String METRIC_MAP_CONFIG_KEY = "metricMap";
  private static final String METRIC_NAME_CONFIG_KEY = "metricName";
  private static final String METRIC_TYPE_CONFIG_KEY = "metricType";
  private static final String METRIC_SCOPE_CONFIG_KEY = "metricScope";

  private final String viewName;
  private final String tenantColumnName;
  private final Map<String, MetricConfig> metricMap;
  private final Map<String, String> columnMap;

  public PrometheusViewDefinition(
      String viewName,
      String tenantColumnName,
      Map<String, MetricConfig> metricMap,
      Map<String, String> columnMap) {
    this.viewName = viewName;
    this.tenantColumnName = tenantColumnName; // tenantAttributeName
    this.metricMap = metricMap;
    this.columnMap = columnMap;
  }

  public static PrometheusViewDefinition parse(Config config, String tenantColumnName) {
    String viewName = config.getString(VIEW_NAME_CONFIG_KEY);

    final Map<String, String> fieldMap = Maps.newHashMap();
    Config fieldMapConfig = config.getConfig(ATTRIBUTE_MAP_CONFIG_KEY);
    for (Entry<String, ConfigValue> element : fieldMapConfig.entrySet()) {
      List<String> keys = ConfigUtil.splitPath(element.getKey());
      fieldMap.put(keys.get(0), fieldMapConfig.getString(element.getKey()));
    }

    final Map<String, MetricConfig> metricMap = Maps.newHashMap();
    Config metricMapConfig = config.getConfig(METRIC_MAP_CONFIG_KEY);

    Set<String> metricNames = Sets.newHashSet();
    for (Entry<String, ConfigValue> element : metricMapConfig.entrySet()) {
      List<String> keys = ConfigUtil.splitPath(element.getKey());
      metricNames.add(keys.get(0));
    }

    String metricScope = config.getString(METRIC_SCOPE_CONFIG_KEY);
    for (String metricName : metricNames) {
      Config metricDef = metricMapConfig.getConfig(metricName);
      metricMap.put(
          metricScope + "." + metricName,
          new MetricConfig(
              metricDef.getString(METRIC_NAME_CONFIG_KEY),
              MetricType.valueOf(metricDef.getString(METRIC_TYPE_CONFIG_KEY))));
    }

    return new PrometheusViewDefinition(
        viewName, tenantColumnName,
        metricMap, fieldMap);
  }

  public String getPhysicalColumnName(String logicalColumnName) {
    return columnMap.get(logicalColumnName);
  }

  public MetricConfig getMetricConfig(String logicalMetricName) {
    return metricMap.get(logicalMetricName);
  }

  public String getViewName() {
    return viewName;
  }

  public String getTenantColumnName() {
    return tenantColumnName;
  }

  @Getter
  @AllArgsConstructor
  public static class MetricConfig {
    private final String name;
    private final MetricType metricType;
  }

  public enum MetricType {
    GAUGE,
    COUNTER
  }
}