package org.hypertrace.core.query.service.trino;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigUtil;
import com.typesafe.config.ConfigValue;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.hypertrace.core.query.service.api.ValueType;

/** Class holding the configuration for a Trino table. */
public class TableDefinition {

  private static final String TABLE_NAME_CONFIG_KEY = "tableName";
  private static final String RETENTION_TIME_CONFIG_KEY = "retentionTimeMillis";
  private static final String TIME_GRANULARITY_CONFIG_KEY = "timeGranularityMillis";
  private static final String ARRAY_FIELDS_CONFIG_KEY = "arrayFields";
  private static final String BYTES_FIELDS_CONFIG_KEY = "bytesFields";
  private static final String TDIGEST_FIELDS_CONFIG_KEY = "tdigestFields";
  private static final String FIELD_MAP_CONFIG_KEY = "fieldMap";
  private static final String MAP_FIELDS_CONFIG_KEY = "mapFields";
  private static final String FILTERS_CONFIG_KEY = "filters";
  private static final String COLUMN_CONFIG_KEY = "column";

  private static final long DEFAULT_RETENTION_TIME = TimeUnit.DAYS.toMillis(8);
  private static final long DEFAULT_TIME_GRANULARITY = TimeUnit.MINUTES.toMillis(1);

  private final String tableName;
  private final long retentionTimeMillis;
  private final long timeGranularityMillis;
  private final Map<String, TrinoColumnSpec> columnSpecMap;

  /**
   * The name of the column which should be used as tenant id. This is configurable so that each
   * view can pick and choose what column is tenant id.
   */
  private final String tenantColumnName;

  /** The name of the column which should be used to get count */
  private final Optional<String> countColumnName;

  /**
   * Map from column name to the ViewColumnFilter that's applied to this view for that column. All
   * the view filters are AND'ed and only the queries matching all the view filters will be routed
   * to this view.
   */
  private final Map<String, TableColumnFilter> columnFilterMap;

  public TableDefinition(
      String tableName,
      long retentionTimeMillis,
      long timeGranularityMillis,
      Map<String, TrinoColumnSpec> columnSpecMap,
      String tenantColumnName,
      Optional<String> countColumnName,
      Map<String, TableColumnFilter> filterMap) {
    this.tableName = tableName;
    this.retentionTimeMillis = retentionTimeMillis;
    this.timeGranularityMillis = timeGranularityMillis;
    this.columnSpecMap = columnSpecMap;
    this.tenantColumnName = tenantColumnName;
    this.countColumnName = countColumnName;
    this.columnFilterMap = filterMap;
  }

  public static TableDefinition parse(
      Config config, String tenantColumnName, Optional<String> countColumnName) {
    String tableName = config.getString(TABLE_NAME_CONFIG_KEY);
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

    // get bytes fields
    final Set<String> tdigestFields =
        new HashSet<>(
            config.hasPath(TDIGEST_FIELDS_CONFIG_KEY)
                ? config.getStringList(TDIGEST_FIELDS_CONFIG_KEY)
                : List.of());

    // get array fields
    final Set<String> arrayFields =
        new HashSet<>(
            config.hasPath(ARRAY_FIELDS_CONFIG_KEY)
                ? config.getStringList(ARRAY_FIELDS_CONFIG_KEY)
                : List.of());

    Map<String, TrinoColumnSpec> columnSpecMap = new HashMap<>();
    for (Entry<String, String> entry : fieldMap.entrySet()) {
      String logicalName = entry.getKey();
      String physName = entry.getValue();
      TrinoColumnSpec spec;
      // todo: replace this with call to attribute service
      if (mapFields.contains(physName)) {
        spec = new TrinoColumnSpec(physName, ValueType.STRING_MAP, false);
      } else if (bytesFields.contains(physName)) {
        spec = new TrinoColumnSpec(physName, ValueType.BYTES, false);
      } else if (tdigestFields.contains(physName)) {
        spec = new TrinoColumnSpec(physName, ValueType.STRING, true);
      } else if (arrayFields.contains(physName)) {
        spec = new TrinoColumnSpec(physName, ValueType.STRING_ARRAY, true);
      } else {
        spec = new TrinoColumnSpec(physName, ValueType.STRING, false);
      }
      columnSpecMap.put(logicalName, spec);
    }

    // Check if there are any view filters. If there are multiple filters, they all will
    // be AND'ed together.
    final Map<String, TableColumnFilter> filterMap = new HashMap<>();
    if (config.hasPath(FILTERS_CONFIG_KEY)) {
      for (Config filterConfig : config.getConfigList(FILTERS_CONFIG_KEY)) {
        filterMap.put(
            filterConfig.getString(COLUMN_CONFIG_KEY), TableColumnFilter.from(filterConfig));
      }
    }

    return new TableDefinition(
        tableName,
        retentionTimeMillis,
        timeGranularityMillis,
        columnSpecMap,
        tenantColumnName,
        countColumnName,
        filterMap);
  }

  public String getTableName() {
    return tableName;
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

  public Optional<String> getCountColumnName() {
    return countColumnName;
  }

  public boolean containsColumn(String referencedColumn) {
    return columnSpecMap.containsKey(referencedColumn)
        || columnFilterMap.containsKey(referencedColumn);
  }

  public String getPhysicalColumnName(String logicalColumnName) {
    return columnSpecMap.get(logicalColumnName).getColumnName();
  }

  public ValueType getColumnType(String logicalName) {
    return columnSpecMap.get(logicalName).getType();
  }

  public boolean isTdigestColumnType(String logicalName) {
    return columnSpecMap.get(logicalName).isTdigest();
  }

  @Nonnull
  public Map<String, TableColumnFilter> getColumnFilterMap() {
    return this.columnFilterMap;
  }
}
