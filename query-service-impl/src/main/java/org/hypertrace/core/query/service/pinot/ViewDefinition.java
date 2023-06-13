package org.hypertrace.core.query.service.pinot;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigUtil;
import com.typesafe.config.ConfigValue;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.hypertrace.core.query.service.api.ValueType;

/** Class holding the configuration for a Pinot view/table. */
public class ViewDefinition {

  static final String MAP_KEYS_SUFFIX = "__KEYS";
  static final String MAP_VALUES_SUFFIX = "__VALUES";

  private static final String VIEW_NAME_CONFIG_KEY = "viewName";
  private static final String RETENTION_TIME_CONFIG_KEY = "retentionTimeMillis";
  private static final String TIME_GRANULARITY_CONFIG_KEY = "timeGranularityMillis";
  private static final String BYTES_FIELDS_CONFIG_KEY = "bytesFields";
  private static final String FIELD_MAP_CONFIG_KEY = "fieldMap";
  private static final String MAP_FIELDS_CONFIG_KEY = "mapFields";
  private static final String FILTERS_CONFIG_KEY = "filters";
  private static final String COLUMN_CONFIG_KEY = "column";
  private static final String TEXT_INDEXES_FIELDS_CONFIG_KEY = "textIndexes";

  private static final long DEFAULT_RETENTION_TIME = TimeUnit.DAYS.toMillis(8);
  private static final long DEFAULT_TIME_GRANULARITY = TimeUnit.MINUTES.toMillis(1);

  private final String viewName;
  private final long retentionTimeMillis;
  private final long timeGranularityMillis;
  private final Map<String, PinotColumnSpec> columnSpecMap;

  /**
   * The name of the column which should be used as tenant id. This is configurable so that each
   * view can pick and choose what column is tenant id.
   */
  private final String tenantColumnName;

  /**
   * Map from column name to the ViewColumnFilter that's applied to this view for that column. All
   * the view filters are AND'ed and only the queries matching all the view filters will be routed
   * to this view.
   */
  private final Map<String, ViewColumnFilter> columnFilterMap;

  public ViewDefinition(
      String viewName,
      long retentionTimeMillis,
      long timeGranularityMillis,
      Map<String, PinotColumnSpec> columnSpecMap,
      String tenantColumnName,
      Map<String, ViewColumnFilter> filterMap) {
    this.viewName = viewName;
    this.retentionTimeMillis = retentionTimeMillis;
    this.timeGranularityMillis = timeGranularityMillis;
    this.columnSpecMap = columnSpecMap;
    this.tenantColumnName = tenantColumnName;
    this.columnFilterMap = filterMap;
  }

  public static ViewDefinition parse(Config config, String tenantColumnName) {
    String viewName = config.getString(VIEW_NAME_CONFIG_KEY);
    long retentionTimeMillis =
        config.hasPath(RETENTION_TIME_CONFIG_KEY)
            ? config.getLong(RETENTION_TIME_CONFIG_KEY)
            : DEFAULT_RETENTION_TIME;
    long timeGranularityMillis =
        config.hasPath(TIME_GRANULARITY_CONFIG_KEY)
            ? config.getLong(TIME_GRANULARITY_CONFIG_KEY)
            : DEFAULT_TIME_GRANULARITY;

    final Map<String, String> fieldMap = new HashMap<>();
    Config fieldMapConfig = config.getConfig(FIELD_MAP_CONFIG_KEY);
    for (Entry<String, ConfigValue> element : fieldMapConfig.entrySet()) {
      // Since the key part is a complete key, instead of an object, we need to handle it
      // specially to avoid including quotes in the key name.
      List<String> keys = ConfigUtil.splitPath(element.getKey());
      fieldMap.put(keys.get(0), fieldMapConfig.getString(element.getKey()));
    }

    final List<String> mapFieldsList =
        config.hasPath(MAP_FIELDS_CONFIG_KEY)
            ? config.getStringList(MAP_FIELDS_CONFIG_KEY)
            : List.of();
    Set<String> mapFields = new HashSet<>();
    if (mapFieldsList != null) {
      mapFields.addAll(mapFieldsList);
    }

    // get bytes fields
    final Set<String> bytesFields =
        new HashSet<>(
            config.hasPath(BYTES_FIELDS_CONFIG_KEY)
                ? config.getStringList(BYTES_FIELDS_CONFIG_KEY)
                : List.of());

    // get all the String fields that have enabled text Indexes
    final Set<String> textIndexFields =
        new HashSet<>(
            config.hasPath(TEXT_INDEXES_FIELDS_CONFIG_KEY)
                ? config.getStringList(TEXT_INDEXES_FIELDS_CONFIG_KEY)
                : List.of());

    Map<String, PinotColumnSpec> columnSpecMap = new HashMap<>();
    for (Map.Entry<String, String> entry : fieldMap.entrySet()) {
      String logicalName = entry.getKey();
      String physName = entry.getValue();
      PinotColumnSpec spec = new PinotColumnSpec();
      // todo: replace this with call to attribute service
      if (mapFields.contains(physName)) {
        spec.setType(ValueType.STRING_MAP);
        // split them to 2 automatically here
        spec.addColumnName(physName + MAP_KEYS_SUFFIX);
        spec.addColumnName(physName + MAP_VALUES_SUFFIX);
      } else if (bytesFields.contains(physName)) {
        spec.addColumnName(physName);
        spec.setType(ValueType.BYTES);
      } else {
        spec.addColumnName(physName);
        spec.setType(ValueType.STRING);
      }

      if (textIndexFields.contains(physName)) {
        spec.setTextIndex();
      }

      columnSpecMap.put(logicalName, spec);
    }

    // Check if there are any view filters. If there are multiple filters, they all will
    // be AND'ed together.
    final Map<String, ViewColumnFilter> filterMap = new HashMap<>();
    if (config.hasPath(FILTERS_CONFIG_KEY)) {
      for (Config filterConfig : config.getConfigList(FILTERS_CONFIG_KEY)) {
        filterMap.put(
            filterConfig.getString(COLUMN_CONFIG_KEY), ViewColumnFilter.from(filterConfig));
      }
    }

    return new ViewDefinition(
        viewName,
        retentionTimeMillis,
        timeGranularityMillis,
        columnSpecMap,
        tenantColumnName,
        filterMap);
  }

  public String getViewName() {
    return viewName;
  }

  public long getRetentionTimeMillis() {
    return retentionTimeMillis;
  }

  public long getTimeGranularityMillis() {
    return timeGranularityMillis;
  }

  public String getTenantIdColumn() {
    return tenantColumnName;
  }

  public boolean containsColumn(String referencedColumn) {
    return columnSpecMap.containsKey(referencedColumn)
        || columnFilterMap.containsKey(referencedColumn);
  }

  public List<String> getPhysicalColumnNames(String logicalColumnName) {
    return columnSpecMap.get(logicalColumnName).getColumnNames();
  }

  public boolean isMap(String logicalName) {
    return (ValueType.STRING_MAP.equals(columnSpecMap.get(logicalName).getType()));
  }

  public ValueType getColumnType(String logicalName) {
    return columnSpecMap.get(logicalName).getType();
  }

  public boolean hasTextIndex(String logicalName) {
    return columnSpecMap.get(logicalName).hasTextIndex();
  }

  public String getKeyColumnNameForMap(String logicalName) {
    List<String> keys = findPhysicalNameWithSuffix(logicalName, MAP_KEYS_SUFFIX);
    Preconditions.checkArgument(keys.size() <= 1);
    return keys.isEmpty() ? null : keys.get(0);
  }

  public String getValueColumnNameForMap(String logicalName) {
    List<String> keys = findPhysicalNameWithSuffix(logicalName, MAP_VALUES_SUFFIX);
    Preconditions.checkArgument(keys.size() <= 1);
    return keys.isEmpty() ? null : keys.get(0);
  }

  private List<String> findPhysicalNameWithSuffix(String logicalName, String suffix) {
    return columnSpecMap.get(logicalName).getColumnNames().stream()
        .filter(e -> e.toUpperCase().endsWith(suffix))
        .collect(Collectors.toList());
  }

  @Nonnull
  public Map<String, ViewColumnFilter> getColumnFilterMap() {
    return this.columnFilterMap;
  }
}
