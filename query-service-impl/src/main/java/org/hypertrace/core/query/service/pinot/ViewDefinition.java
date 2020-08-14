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
import java.util.stream.Collectors;
import org.hypertrace.core.query.service.api.ValueType;

public class ViewDefinition {

  static final String MAP_KEYS_SUFFIX = "__KEYS";
  static final String MAP_VALUES_SUFFIX = "__VALUES";

  private final String viewName;
  private final Map<String, PinotColumnSpec> columnSpecMap;
  private final String tenantColumnName;

  public ViewDefinition(
      String viewName, Map<String, PinotColumnSpec> columnSpecMap, String tenantColumnName) {
    this.viewName = viewName;
    this.columnSpecMap = columnSpecMap;
    this.tenantColumnName = tenantColumnName;
  }

  public static ViewDefinition parse(Config config, String tenantColumnName) {
    String viewName = config.getString("viewName");
    final Map<String, String> fieldMap = new HashMap<>();
    Config fieldMapConfig = config.getConfig("fieldMap");
    for (Entry<String, ConfigValue> element : fieldMapConfig.entrySet()) {
      // Since the key part is a complete key, instead of an object, we need to handle it
      // specially to avoid including quotes in the key name.
      List<String> keys = ConfigUtil.splitPath(element.getKey());
      fieldMap.put(keys.get(0), fieldMapConfig.getString(element.getKey()));
    }

    final List<String> mapFieldsList =
        config.hasPath("mapFields") ? config.getStringList("mapFields") : List.of();
    Set<String> mapFields = new HashSet<>();
    if (mapFieldsList != null) {
      mapFields.addAll(mapFieldsList);
    }

    // get bytes fields
    final Set<String> bytesFields = new HashSet<>(
        config.hasPath("bytesFields") ? config.getStringList("bytesFields") : List.of());

    Map<String, PinotColumnSpec> columnSpecMap = new HashMap<>();
    for (String logicalName : fieldMap.keySet()) {
      String physName = fieldMap.get(logicalName);
      PinotColumnSpec spec = new PinotColumnSpec();
      // todo: replace this with call to attribute service
      if (mapFields.contains(fieldMap.get(logicalName))) {
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
      columnSpecMap.put(logicalName, spec);
    }
    return new ViewDefinition(viewName, columnSpecMap, tenantColumnName);
  }

  public String getViewName() {
    return viewName;
  }

  public String getTenantIdColumn() {
    return tenantColumnName;
  }

  public boolean containsColumn(String referencedColumn) {
    return columnSpecMap.containsKey(referencedColumn);
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
}
