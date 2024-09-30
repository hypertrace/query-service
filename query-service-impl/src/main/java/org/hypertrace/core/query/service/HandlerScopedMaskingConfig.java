package org.hypertrace.core.query.service;

import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.apache.pinot.client.ResultSet;
import org.hypertrace.core.query.service.api.Row;

@Slf4j
public class HandlerScopedMaskingConfig {
  private static final String TENANT_SCOPED_MASKS_CONFIG_KEY = "tenantScopedMaskingCriteria";
  private final Map<String, List<MaskValuesForTimeRange>> tenantToMaskValuesMap;
  private String tenantId;
  private final String timeFilterColumnName;
  private int timeFilterColumnIndex;
  private final HashMap<String, Integer> columnNameToIndexMap = new HashMap<>();

  List<MaskValuesForTimeRange> getMaskValues(String tenantId) {
    if (tenantToMaskValuesMap.containsKey(tenantId)) return tenantToMaskValuesMap.get(tenantId);

    return new ArrayList<>();
  }

  public HandlerScopedMaskingConfig(
      Config config, String timeFilterColumnName, String tenantColumnName) {
    if (config.hasPath(TENANT_SCOPED_MASKS_CONFIG_KEY)) {
      this.tenantToMaskValuesMap =
          config.getConfigList(TENANT_SCOPED_MASKS_CONFIG_KEY).stream()
              .map(maskConfig -> new TenantMasks(maskConfig, timeFilterColumnName))
              .collect(
                  Collectors.toMap(
                      tenantFilters -> tenantFilters.tenantId,
                      tenantFilters -> tenantFilters.maskValues));
    } else {
      this.tenantToMaskValuesMap = Collections.emptyMap();
    }

    this.timeFilterColumnName = timeFilterColumnName;
  }

  public void parseColumns(ResultSet resultSet, String tenantId) {
    timeFilterColumnIndex = -1;
    columnNameToIndexMap.clear();
    this.tenantId = tenantId;

    for (int colIdx = 0; colIdx < resultSet.getColumnCount(); colIdx++) {
      String temp = resultSet.getColumnName(colIdx);
      if (Objects.equals(this.timeFilterColumnName, resultSet.getColumnName(colIdx))) {
        this.timeFilterColumnIndex = colIdx;
      }

      columnNameToIndexMap.put(resultSet.getColumnName(colIdx), colIdx);
    }
  }

  public Row mask(Row row) {
    if (this.timeFilterColumnIndex == -1) {
      return row;
    }

    List<MaskValuesForTimeRange> masks = getMaskValues(this.tenantId);
    if (masks.isEmpty()) {
      return row;
    }

    Row.Builder maskedRowBuilder = Row.newBuilder(row);

    for (MaskValuesForTimeRange mask : masks) {
      boolean toBeMasked = true;
      if (mask.getEndTimeMillis().isPresent()) {
        if (mask.getEndTimeMillis().get() < row.getColumn(this.timeFilterColumnIndex).getLong()) {
          toBeMasked = false;
        }
      }
      if (mask.getStartTimeMillis().isPresent()) {
        if (mask.getStartTimeMillis().get() > row.getColumn(this.timeFilterColumnIndex).getLong()) {
          toBeMasked = false;
        }
      }

      if (toBeMasked) {
        for (String columnName : mask.maskValues.getColumnToMaskedValue().keySet()) {
          int colIdx = columnNameToIndexMap.get(columnName);
          org.hypertrace.core.query.service.api.Value value =
              org.hypertrace.core.query.service.api.Value.newBuilder()
                  .setString(mask.maskValues.getColumnToMaskedValue().get(columnName))
                  .build();
          maskedRowBuilder.setColumn(colIdx, value);
        }
      }
    }

    return maskedRowBuilder.build();
  }

  @Value
  @NonFinal
  private class TenantMasks {
    private static final String TENANT_ID_CONFIG_KEY = "tenantId";
    private static final String TIME_RANGE_AND_MASK_VALUES_CONFIG_KEY = "timeRangeAndMaskValues";
    String tenantId;
    String startTimeAttributeName;
    List<MaskValuesForTimeRange> maskValues;

    private TenantMasks(Config config, String startTimeAttributeName) {
      this.tenantId = config.getString(TENANT_ID_CONFIG_KEY);
      this.startTimeAttributeName = startTimeAttributeName;
      this.maskValues =
          config.getConfigList(TIME_RANGE_AND_MASK_VALUES_CONFIG_KEY).stream()
              .map(MaskValuesForTimeRange::new)
              .collect(Collectors.toList());
    }
  }

  @Value
  private class MaskValues {
    Map<String, String> columnToMaskedValue;

    MaskValues(Map<String, String> columnToMaskedValue) {
      this.columnToMaskedValue = columnToMaskedValue;
    }
  }

  @Value
  @NonFinal
  class MaskValuesForTimeRange {
    private static final String START_TIME_CONFIG_PATH = "startTimeMillis";
    private static final String END_TIME_CONFIG_PATH = "endTimeMillis";
    private static final String MASK_VALUE_CONFIG_PATH = "maskValues";
    private static final String ATTRIBUTE_ID_CONFIG_PATH = "attributeId";
    private static final String MASKED_VALUE_CONFIG_PATH = "maskedValue";
    Optional<Long> startTimeMillis;
    Optional<Long> endTimeMillis;
    MaskValues maskValues;

    private MaskValuesForTimeRange(Config config) {
      if (config.hasPath(START_TIME_CONFIG_PATH) && config.hasPath(END_TIME_CONFIG_PATH)) {
        this.startTimeMillis = Optional.of(config.getLong(START_TIME_CONFIG_PATH));
        this.endTimeMillis = Optional.of(config.getLong(END_TIME_CONFIG_PATH));
      } else {
        startTimeMillis = Optional.empty();
        endTimeMillis = Optional.empty();
      }
      if (config.hasPath(MASK_VALUE_CONFIG_PATH)) {
        List<Config> maskedValuesList =
            new ArrayList<>(config.getConfigList(MASK_VALUE_CONFIG_PATH));
        HashMap<String, String> maskedValuesMap = new HashMap<>();
        maskedValuesList.forEach(
            maskedValue -> {
              maskedValuesMap.put(
                  maskedValue.getString(ATTRIBUTE_ID_CONFIG_PATH),
                  maskedValue.getString(MASKED_VALUE_CONFIG_PATH));
            });

        maskValues = new MaskValues(maskedValuesMap);
      } else {
        maskValues = new MaskValues(new HashMap<>());
      }
    }
  }
}
