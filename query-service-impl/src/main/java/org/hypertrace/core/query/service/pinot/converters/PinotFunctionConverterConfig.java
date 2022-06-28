package org.hypertrace.core.query.service.pinot.converters;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class PinotFunctionConverterConfig {

  private static final String PERCENTILE_AGGREGATION_FUNCTION_CONFIG = "percentileAggFunction";
  private static final String DISTINCT_COUNT_AGGREGATION_FUNCTION_CONFIG =
      "distinctCountAggFunction";
  private static final String DEFAULT_PERCENTILE_AGGREGATION_FUNCTION = "PERCENTILETDIGEST";
  private static final String DEFAULT_DISTINCT_COUNT_AGGREGATION_FUNCTION = "DISTINCTCOUNT";

  String percentileAggregationFunction;
  String distinctCountAggregationFunction;

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
  }

  public PinotFunctionConverterConfig() {
    this(ConfigFactory.empty());
  }
}
