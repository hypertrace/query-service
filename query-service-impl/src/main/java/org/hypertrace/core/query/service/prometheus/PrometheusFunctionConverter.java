package org.hypertrace.core.query.service.prometheus;

import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_AVG;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_COUNT;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_MAX;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_MIN;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_SUM;

import org.hypertrace.core.query.service.api.Expression;

class PrometheusFunctionConverter {

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
                "Couldn't map query function {} to prometheus function", queryFunctionName));
    }
  }
}
