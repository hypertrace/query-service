package org.hypertrace.core.query.service.prometheus;

import org.hypertrace.core.query.service.QueryRequestUtil;
import org.hypertrace.core.query.service.api.Expression;

class PrometheusUtils {
  static String generateAliasForMetricFunction(Expression functionExpression) {
    String functionName = functionExpression.getFunction().getFunctionName().toUpperCase();
    String columnName =
        QueryRequestUtil.getLogicalColumnName(functionExpression.getFunction().getArguments(0))
            .orElseThrow(IllegalArgumentException::new);
    return String.join(":", functionName, columnName);
  }
}
