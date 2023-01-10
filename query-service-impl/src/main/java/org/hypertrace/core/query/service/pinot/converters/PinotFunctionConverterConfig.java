package org.hypertrace.core.query.service.pinot.converters;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class PinotFunctionConverterConfig {

  private static final String PERCENTILE_AGGREGATION_FUNCTION_CONFIG = "percentileAggFunction";
  private static final String DISTINCT_COUNT_AGGREGATION_FUNCTION_CONFIG =
      "distinctCountAggFunction";
  private static final String DISTINCT_COUNT_AGGREGATION_OVERRIDES = "distinctCountAggOverrides";
  private static final String DEFAULT_PERCENTILE_AGGREGATION_FUNCTION = "PERCENTILETDIGEST";
  private static final String DEFAULT_DISTINCT_COUNT_AGGREGATION_FUNCTION = "DISTINCTCOUNT";

  String percentileAggregationFunction;
  String distinctCountAggregationFunction;
  @Nonnull Map<String, String> distinctCountAggOverrides;

  public PinotFunctionConverterConfig(Config config) {
    if (config.hasPath(PERCENTILE_AGGREGATION_FUNCTION_CONFIG)) {
      this.percentileAggregationFunction = config.getString(PERCENTILE_AGGREGATION_FUNCTION_CONFIG);
    } else {
      this.percentileAggregationFunction = DEFAULT_PERCENTILE_AGGREGATION_FUNCTION;
    }
    if (config.hasPath(DISTINCT_COUNT_AGGREGATION_FUNCTION_CONFIG)) {
      this.distinctCountAggregationFunction =
          config.getString(DISTINCT_COUNT_AGGREGATION_FUNCTION_CONFIG);
    } else {
      this.distinctCountAggregationFunction = DEFAULT_DISTINCT_COUNT_AGGREGATION_FUNCTION;
    }
    if (config.hasPath(DISTINCT_COUNT_AGGREGATION_OVERRIDES)) {
      Config overridesConfig = config.getConfig(DISTINCT_COUNT_AGGREGATION_OVERRIDES);
      this.distinctCountAggOverrides =
          overridesConfig.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      entry -> entry.getKey(), entry -> overridesConfig.getString(entry.getKey())));
    } else {
      this.distinctCountAggOverrides = Collections.emptyMap();
    }
  }

  public PinotFunctionConverterConfig() {
    this(ConfigFactory.empty());
  }

  public String getDistinctCountFunction(String arg) {
    return Optional.ofNullable(distinctCountAggOverrides.get(arg))
        .orElse(distinctCountAggregationFunction);
  }
}
