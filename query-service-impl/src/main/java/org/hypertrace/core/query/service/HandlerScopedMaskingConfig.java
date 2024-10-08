package org.hypertrace.core.query.service;

import com.typesafe.config.Config;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
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

  public List<String> getMaskedAttributes(ExecutionContext executionContext) {
    String tenantId = executionContext.getTenantId();
    List<String> maskedAttributes = new ArrayList<>();
    //    maskedValue.clear();
    if (!tenantToMaskValuesMap.containsKey(tenantId)) {
      return maskedAttributes;
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
        maskedAttributes.addAll(timeRangeAndMasks.maskedAttributes);
      }
    }

    return maskedAttributes;
  }

  private static boolean isTimeRangeOverlap(
      MaskValuesForTimeRange timeRangeAndMasks, Instant queryStartTime, Instant queryEndTime) {
    boolean timeRangeOverlap = true;

    if (timeRangeAndMasks.getStartTimeMillis().isPresent()) {
      Instant startTimeInstant = Instant.ofEpochMilli(timeRangeAndMasks.getStartTimeMillis().get());
      if (startTimeInstant.isBefore(queryStartTime) || startTimeInstant.isAfter(queryEndTime)) {
        timeRangeOverlap = false;
      }
    }

    if (timeRangeAndMasks.getEndTimeMillis().isPresent()) {
      Instant endTimeInstant = Instant.ofEpochMilli(timeRangeAndMasks.getEndTimeMillis().get());
      if (endTimeInstant.isBefore(queryStartTime) || endTimeInstant.isAfter(queryEndTime)) {
        timeRangeOverlap = false;
      }
    }
    return timeRangeOverlap;
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
              .filter(MaskValuesForTimeRange::isValid)
              .collect(Collectors.toList());
    }
  }

  @Value
  @NonFinal
  class MaskValuesForTimeRange {
    private static final String START_TIME_CONFIG_PATH = "startTimeMillis";
    private static final String END_TIME_CONFIG_PATH = "endTimeMillis";
    private static final String MASK_ATTRIBUTES_CONFIG_PATH = "maskedAttributes";
    Optional<Long> startTimeMillis;
    Optional<Long> endTimeMillis;
    ArrayList<String> maskedAttributes;

    private MaskValuesForTimeRange(Config config) {
      if (config.hasPath(START_TIME_CONFIG_PATH) && config.hasPath(END_TIME_CONFIG_PATH)) {
        this.startTimeMillis = Optional.of(config.getLong(START_TIME_CONFIG_PATH));
        this.endTimeMillis = Optional.of(config.getLong(END_TIME_CONFIG_PATH));
      } else {
        startTimeMillis = Optional.empty();
        endTimeMillis = Optional.empty();
        log.warn(
            "A masking filter is provided without startTimeMillis or endTimeMillis in tenantScopedMaskingCriteria. This filter will be ignored.");
      }
      if (config.hasPath(MASK_ATTRIBUTES_CONFIG_PATH)) {
        maskedAttributes = new ArrayList<>(config.getStringList(MASK_ATTRIBUTES_CONFIG_PATH));
      } else {
        maskedAttributes = new ArrayList<>();
      }
    }

    boolean isValid() {
      return startTimeMillis.isPresent() && endTimeMillis.isPresent();
    }
  }
}
