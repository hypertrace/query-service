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
  private static final String DATE_TIME_CONVERT_FUNCTION_CONFIG = "dateTimeConvertFunction";
  private static final String DISTINCT_COUNT_AGGREGATION_FUNCTION_CONFIG =
      "distinctCountAggFunction";
  private static final String UNNEST_FUNCTION_CONFIG = "unnestFunction";
  private static final String DEFAULT_PERCENTILE_AGGREGATION_FUNCTION =
      "percentile_cont(%f) within group (order by (%s) asc)";
  private static final String DEFAULT_TDIGEST_PERCENTILE_AGGREGATION_FUNCTION =
      "tdigest_percentile";
  private static final String DEFAULT_DATE_TIME_CONVERT_FUNCTION = "dateTimeConvert";
  private static final String DEFAULT_DISTINCT_COUNT_AGGREGATION_FUNCTION = "count(distinct %s)";
  private static final String DEFAULT_UNNEST_FUNCTION = "unnest(%s)";

  String percentileAggregationFunction;
  String tdigestPercentileAggregationFunction;
  String dateTimeConvertFunction;
  String distinctCountAggregationFunction;
  String unnestFunction;

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
    if (config.hasPath(DATE_TIME_CONVERT_FUNCTION_CONFIG)) {
      this.dateTimeConvertFunction = config.getString(DATE_TIME_CONVERT_FUNCTION_CONFIG);
    } else {
      this.dateTimeConvertFunction = DEFAULT_DATE_TIME_CONVERT_FUNCTION;
    }
    if (config.hasPath(DISTINCT_COUNT_AGGREGATION_FUNCTION_CONFIG)) {
      this.distinctCountAggregationFunction =
          config.getString(DISTINCT_COUNT_AGGREGATION_FUNCTION_CONFIG);
    } else {
      this.distinctCountAggregationFunction = DEFAULT_DISTINCT_COUNT_AGGREGATION_FUNCTION;
    }
    if (config.hasPath(UNNEST_FUNCTION_CONFIG)) {
      this.unnestFunction = config.getString(UNNEST_FUNCTION_CONFIG);
    } else {
      this.unnestFunction = DEFAULT_UNNEST_FUNCTION;
    }
  }

  public PostgresFunctionConverterConfig() {
    this(ConfigFactory.empty());
  }
}
