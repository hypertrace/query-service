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
import lombok.Value;

/** Prometheus metric & attribute mapping for a pinot view */
class PrometheusViewDefinition {

  private static final String VIEW_NAME_CONFIG_KEY = "viewName";
  private static final String ATTRIBUTE_MAP_CONFIG_KEY = "attributeMap";
  private static final String METRIC_MAP_CONFIG_KEY = "metricMap";
  private static final String METRIC_NAME_CONFIG_KEY = "metricName";
  private static final String METRIC_TYPE_CONFIG_KEY = "metricType";
  private static final String METRIC_SCOPE_CONFIG_KEY = "metricScope";

  private final String viewName;
  private final String tenantAttributeName;
  private final Map<String, MetricConfig> metricMap;
  private final Map<String, String> attributeMap;

  public PrometheusViewDefinition(
      String viewName,
      String tenantColumnName,
      Map<String, MetricConfig> metricMap,
      Map<String, String> columnMap) {
    this.viewName = viewName;
    this.tenantAttributeName = tenantColumnName;
    this.metricMap = metricMap;
    this.attributeMap = columnMap;
  }

  public static PrometheusViewDefinition parse(Config config, String tenantColumnName) {
    String viewName = config.getString(VIEW_NAME_CONFIG_KEY);

    final Map<String, String> fieldMap = Maps.newHashMap();
    Config fieldMapConfig = config.getConfig(ATTRIBUTE_MAP_CONFIG_KEY);
    for (Entry<String, ConfigValue> element : fieldMapConfig.entrySet()) {
      List<String> keys = ConfigUtil.splitPath(element.getKey());
      fieldMap.put(keys.get(0), fieldMapConfig.getString(element.getKey()));
    }

    Config metricMapConfig = config.getConfig(METRIC_MAP_CONFIG_KEY);

    Set<String> metricNames = Sets.newHashSet();
    for (Entry<String, ConfigValue> element : metricMapConfig.entrySet()) {
      List<String> keys = ConfigUtil.splitPath(element.getKey());
      metricNames.add(keys.get(0));
    }

    final Map<String, MetricConfig> metricMap = Maps.newHashMap();
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

  public String getPhysicalColumnNameForLogicalColumnName(String logicalColumnName) {
    return attributeMap.get(logicalColumnName);
  }

  public MetricConfig getMetricConfigForLogicalMetricName(String logicalMetricName) {
    return metricMap.get(logicalMetricName);
  }

  public String getViewName() {
    return viewName;
  }

  public String getTenantAttributeName() {
    return tenantAttributeName;
  }

  @Value
  @AllArgsConstructor
  public static class MetricConfig {
    String metricName;
    MetricType metricType;
  }

  public enum MetricType {
    GAUGE,
    COUNTER
  }
}
