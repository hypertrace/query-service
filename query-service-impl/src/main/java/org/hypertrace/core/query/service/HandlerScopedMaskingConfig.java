package org.hypertrace.core.query.service;

import com.typesafe.config.Config;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HandlerScopedMaskingConfig {
  private static final String TENANT_SCOPED_MASKS_CONFIG_KEY = "tenantScopedMaskingCriteria";
  private final Map<String, List<MaskValuesForTimeRange>> tenantToMaskValuesMap;
  private HashMap<String, Boolean> shouldMaskAttribute = new HashMap<>();
  private HashMap<String, String> maskedValue = new HashMap<>();

  public HandlerScopedMaskingConfig(Config config) {
    if (config.hasPath(TENANT_SCOPED_MASKS_CONFIG_KEY)) {
      this.tenantToMaskValuesMap =
          config.getConfigList(TENANT_SCOPED_MASKS_CONFIG_KEY).stream()
              .map(maskConfig -> new TenantMasks(maskConfig))
              .collect(
                  Collectors.toMap(
                      tenantFilters -> tenantFilters.tenantId,
                      tenantFilters -> tenantFilters.maskValues));
    } else {
      this.tenantToMaskValuesMap = Collections.emptyMap();
    }
  }

  public void parseColumns(ExecutionContext executionContext) {
    shouldMaskAttribute.clear();
    String tenantId = executionContext.getTenantId();

    if (!tenantToMaskValuesMap.containsKey(tenantId)) {
      return;
    }

    Optional<QueryTimeRange> queryTimeRange = executionContext.getQueryTimeRange();
    Instant queryStartTime, queryEndTime;
    if (queryTimeRange.isPresent()) {
      queryStartTime = queryTimeRange.get().getStartTime();
      queryEndTime = queryTimeRange.get().getEndTime();
    } else {
      queryEndTime = Instant.MAX;
      queryStartTime = Instant.MIN;
    }
    for (MaskValuesForTimeRange timeRangeAndMasks : tenantToMaskValuesMap.get(tenantId)) {
      boolean timeRangeOverlap =
          isTimeRangeOverlap(timeRangeAndMasks, queryStartTime, queryEndTime);

      if (timeRangeOverlap) {
        Map<String, String> attributeToMaskedValue =
            timeRangeAndMasks.maskValues.attributeToMaskedValue;
        for (String attribute : attributeToMaskedValue.keySet()) {
          shouldMaskAttribute.put(attribute, true);
          maskedValue.put(attribute, attributeToMaskedValue.get(attribute));
        }
      }
    }
  }

  private static boolean isTimeRangeOverlap(
      MaskValuesForTimeRange timeRangeAndMasks, Instant queryStartTime, Instant queryEndTime) {
    boolean timeRangeOverlap = true;

    if (timeRangeAndMasks.getStartTimeMillis().isPresent()) {
      Instant startTimeInstant = Instant.ofEpochMilli(timeRangeAndMasks.getStartTimeMillis().get());
      if (startTimeInstant.isBefore(queryStartTime) || startTimeInstant.isAfter(queryEndTime)) {
        timeRangeOverlap = false;
      }

      Instant endTimeInstant = Instant.ofEpochMilli(timeRangeAndMasks.getStartTimeMillis().get());
      if (endTimeInstant.isBefore(queryStartTime) || endTimeInstant.isAfter(queryEndTime)) {
        timeRangeOverlap = false;
      }
    }
    return timeRangeOverlap;
  }

  public boolean shouldMask(String attributeName) {
    return this.maskedValue.containsKey(attributeName);
  }

  public String getMaskedValue(String attributeName) {
    return this.maskedValue.get(attributeName);
  }

  @Value
  @NonFinal
  private class TenantMasks {
    private static final String TENANT_ID_CONFIG_KEY = "tenantId";
    private static final String TIME_RANGE_AND_MASK_VALUES_CONFIG_KEY = "timeRangeAndMaskValues";
    String tenantId;
    List<MaskValuesForTimeRange> maskValues;

    private TenantMasks(Config config) {
      this.tenantId = config.getString(TENANT_ID_CONFIG_KEY);
      this.maskValues =
          config.getConfigList(TIME_RANGE_AND_MASK_VALUES_CONFIG_KEY).stream()
              .map(MaskValuesForTimeRange::new)
              .collect(Collectors.toList());
    }
  }

  @Value
  private class MaskValues {
    Map<String, String> attributeToMaskedValue;

    MaskValues(Map<String, String> columnToMaskedValue) {
      this.attributeToMaskedValue = columnToMaskedValue;
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
