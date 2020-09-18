package org.hypertrace.core.query.service;

import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Function;

/** A converter from the canonical format received in a query to the appropriate dat */
public interface QueryFunctionConstants {
  String QUERY_FUNCTION_SUM = "SUM";
  String QUERY_FUNCTION_AVG = "AVG";
  String QUERY_FUNCTION_MIN = "MIN";
  String QUERY_FUNCTION_MAX = "MAX";
  String QUERY_FUNCTION_COUNT = "COUNT";
  String QUERY_FUNCTION_PERCENTILE = "PERCENTILE";
  String QUERY_FUNCTION_DISTINCTCOUNT = "DISTINCTCOUNT";
}