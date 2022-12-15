package org.hypertrace.core.query.service.pinot.converters;

import com.google.common.collect.ImmutableSet;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Collections;
import java.util.Set;
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
  private static final String ARGS_FOR_ACCURATE_DISTINCT_COUNT_AGG_CONFIG =
      "argsForAccurateDistinctCountAgg";
  private static final String DEFAULT_PERCENTILE_AGGREGATION_FUNCTION = "PERCENTILETDIGEST";
  private static final String ACCURATE_DISTINCT_COUNT_AGGREGATION_FUNCTION = "DISTINCTCOUNT";

  String percentileAggregationFunction;
  String distinctCountAggregationFunction;
  @Nonnull Set<String> argsForAccurateDistinctCountAgg;

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
      this.distinctCountAggregationFunction = ACCURATE_DISTINCT_COUNT_AGGREGATION_FUNCTION;
    }
    if (config.hasPath(ARGS_FOR_ACCURATE_DISTINCT_COUNT_AGG_CONFIG)) {
      this.argsForAccurateDistinctCountAgg =
          ImmutableSet.copyOf(config.getStringList(ARGS_FOR_ACCURATE_DISTINCT_COUNT_AGG_CONFIG));
    } else {
      this.argsForAccurateDistinctCountAgg = Collections.emptySet();
    }
  }

  public PinotFunctionConverterConfig() {
    this(ConfigFactory.empty());
  }

  public String getDistinctCountFunction(String arg) {
    if (argsForAccurateDistinctCountAgg.contains(arg)) {
      return ACCURATE_DISTINCT_COUNT_AGGREGATION_FUNCTION;
    }
    return distinctCountAggregationFunction;
  }
}
