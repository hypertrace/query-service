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
  private static final String TENANT_SCOPED_MASKS_CONFIG_KEY = "tenantScopedMaskingConfig";
  private Map<String, List<TimeRangeToMaskedAttributes>> tenantToTimeRangeMaskedAttributes =
      Collections.emptyMap();

  public HandlerScopedMaskingConfig(Config config) {
    if (config.hasPath(TENANT_SCOPED_MASKS_CONFIG_KEY)) {
      this.tenantToTimeRangeMaskedAttributes =
          config.getConfigList(TENANT_SCOPED_MASKS_CONFIG_KEY).stream()
              .map(TenantMaskingConfig::new)
              .collect(
                  Collectors.toMap(
                      TenantMaskingConfig::getTenantId,
                      TenantMaskingConfig::getTimeRangeToMaskedAttributes));
    }
  }

  public List<String> getMaskedAttributes(ExecutionContext executionContext) {
    String tenantId = executionContext.getTenantId();
    List<String> maskedAttributes = new ArrayList<>();
    if (!tenantToTimeRangeMaskedAttributes.containsKey(tenantId)) {
      return maskedAttributes;
    }

    Optional<QueryTimeRange> queryTimeRange = executionContext.getQueryTimeRange();
    Instant queryStartTime = Instant.MIN, queryEndTime = Instant.MAX;
    if (queryTimeRange.isPresent()) {
      queryStartTime = queryTimeRange.get().getStartTime();
      queryEndTime = queryTimeRange.get().getEndTime();
    }
    for (TimeRangeToMaskedAttributes timeRangeAndMasks :
        tenantToTimeRangeMaskedAttributes.get(tenantId)) {
      if (isTimeRangeOverlap(timeRangeAndMasks, queryStartTime, queryEndTime)) {
        maskedAttributes.addAll(timeRangeAndMasks.maskedAttributes);
      }
    }
    return maskedAttributes;
  }

  private static boolean isTimeRangeOverlap(
      TimeRangeToMaskedAttributes timeRangeAndMasks, Instant queryStartTime, Instant queryEndTime) {
    return !(timeRangeAndMasks.startTimeMillis.isAfter(queryEndTime)
        || timeRangeAndMasks.endTimeMillis.isBefore(queryStartTime));
  }

  @Value
  @NonFinal
  static class TenantMaskingConfig {
    private static final String TENANT_ID_CONFIG_KEY = "tenantId";
    private static final String TIME_RANGE_AND_MASK_VALUES_CONFIG_KEY =
        "timeRangeToMaskedAttributes";
    String tenantId;
    List<TimeRangeToMaskedAttributes> timeRangeToMaskedAttributes;

    private TenantMaskingConfig(Config config) {
      this.tenantId = config.getString(TENANT_ID_CONFIG_KEY);
      this.timeRangeToMaskedAttributes =
          config.getConfigList(TIME_RANGE_AND_MASK_VALUES_CONFIG_KEY).stream()
              .map(TimeRangeToMaskedAttributes::new)
              .filter(
                  timeRangeToMaskedAttributes -> {
                    if (!timeRangeToMaskedAttributes.isValid()) {
                      log.warn(
                          "Invalid masking configuration for tenant: {}. Either the time range is missing or the mask list is empty.",
                          this.tenantId);
                      return false;
                    }
                    return true;
                  })
              .collect(Collectors.toList());
    }
  }

  @NonFinal
  static class TimeRangeToMaskedAttributes {
    private static final String START_TIME_CONFIG_PATH = "startTimeMillis";
    private static final String END_TIME_CONFIG_PATH = "endTimeMillis";
    private static final String MASK_ATTRIBUTES_CONFIG_PATH = "maskedAttributes";
    Instant startTimeMillis = null;
    Instant endTimeMillis = null;
    ArrayList<String> maskedAttributes = new ArrayList<>();

    private TimeRangeToMaskedAttributes(Config config) {
      if (config.hasPath(START_TIME_CONFIG_PATH) && config.hasPath(END_TIME_CONFIG_PATH)) {
        Instant startTimeMillis = Instant.ofEpochMilli(config.getLong(START_TIME_CONFIG_PATH));
        Instant endTimeMillis = Instant.ofEpochMilli(config.getLong(END_TIME_CONFIG_PATH));

        if (startTimeMillis.isBefore(endTimeMillis)) {
          this.startTimeMillis = startTimeMillis;
          this.endTimeMillis = endTimeMillis;
          if (config.hasPath(MASK_ATTRIBUTES_CONFIG_PATH)) {
            maskedAttributes = new ArrayList<>(config.getStringList(MASK_ATTRIBUTES_CONFIG_PATH));
          }
        }
      }
    }

    boolean isValid() {
      return startTimeMillis != null && endTimeMillis != null && !maskedAttributes.isEmpty();
    }
  }
}
