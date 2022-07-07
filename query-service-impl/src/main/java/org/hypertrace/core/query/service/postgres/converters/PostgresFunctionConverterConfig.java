package org.hypertrace.core.query.service.postgres.converters;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class PostgresFunctionConverterConfig {

  private static final String PERCENTILE_AGGREGATION_FUNCTION_CONFIG = "percentileAggFunction";
  private static final String TDIGEST_PERCENTILE_AGGREGATION_FUNCTION_CONFIG =
      "tdigestPercentileAggFunction";
  private static final String DISTINCT_COUNT_AGGREGATION_FUNCTION_CONFIG =
      "distinctCountAggFunction";
  // Todo: define this percentile function
  private static final String DEFAULT_PERCENTILE_AGGREGATION_FUNCTION = "percentile_cont";
  private static final String DEFAULT_TDIGEST_PERCENTILE_AGGREGATION_FUNCTION =
      "tdigest_percentile";
  private static final String DEFAULT_DISTINCT_COUNT_AGGREGATION_FUNCTION = "DISTINCTCOUNT";

  String percentileAggregationFunction;
  String tdigestPercentileAggregationFunction;
  String distinctCountAggregationFunction;

  public PostgresFunctionConverterConfig(Config config) {
    if (config.hasPath(PERCENTILE_AGGREGATION_FUNCTION_CONFIG)) {
      this.percentileAggregationFunction = config.getString(PERCENTILE_AGGREGATION_FUNCTION_CONFIG);
    } else {
      this.percentileAggregationFunction = DEFAULT_PERCENTILE_AGGREGATION_FUNCTION;
    }
    if (config.hasPath(TDIGEST_PERCENTILE_AGGREGATION_FUNCTION_CONFIG)) {
      this.tdigestPercentileAggregationFunction =
          config.getString(TDIGEST_PERCENTILE_AGGREGATION_FUNCTION_CONFIG);
    } else {
      this.tdigestPercentileAggregationFunction = DEFAULT_TDIGEST_PERCENTILE_AGGREGATION_FUNCTION;
    }
    if (config.hasPath(DISTINCT_COUNT_AGGREGATION_FUNCTION_CONFIG)) {
      this.distinctCountAggregationFunction =
          config.getString(DISTINCT_COUNT_AGGREGATION_FUNCTION_CONFIG);
    } else {
      this.distinctCountAggregationFunction = DEFAULT_DISTINCT_COUNT_AGGREGATION_FUNCTION;
    }
  }

  public PostgresFunctionConverterConfig() {
    this(ConfigFactory.empty());
  }

  public String getPercentileAggregationFunction(boolean isTdigest) {
    if (isTdigest) {
      return tdigestPercentileAggregationFunction;
    }
    return percentileAggregationFunction;
  }
}
