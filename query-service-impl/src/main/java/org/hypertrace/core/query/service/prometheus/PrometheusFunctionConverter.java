package org.hypertrace.core.query.service.prometheus;

import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_AVG;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_COUNT;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_MAX;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_MIN;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_SUM;

import java.util.List;
import org.hypertrace.core.query.service.api.Expression;

class PrometheusFunctionConverter {

  static final List<String> supportedFunctions =
      List.of(
          QUERY_FUNCTION_SUM,
          QUERY_FUNCTION_MAX,
          QUERY_FUNCTION_MIN,
          QUERY_FUNCTION_AVG,
          QUERY_FUNCTION_COUNT);

  String mapToPrometheusFunctionName(Expression functionSelection) {
    String queryFunctionName = functionSelection.getFunction().getFunctionName().toUpperCase();
    switch (queryFunctionName) {
      case QUERY_FUNCTION_SUM:
        return "sum";
      case QUERY_FUNCTION_MAX:
        return "max";
      case QUERY_FUNCTION_MIN:
        return "min";
      case QUERY_FUNCTION_AVG:
        return "avg";
      case QUERY_FUNCTION_COUNT:
        return "count";
      default:
        throw new RuntimeException(
            String.format(
                "Couldn't map query function [%s] to prometheus function", queryFunctionName));
    }
  }
}
