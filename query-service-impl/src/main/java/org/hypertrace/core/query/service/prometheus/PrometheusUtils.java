package org.hypertrace.core.query.service.prometheus;

import org.hypertrace.core.query.service.QueryRequestUtil;
import org.hypertrace.core.query.service.api.Expression;

class PrometheusUtils {
  static String getColumnNameForMetricFunction(Expression functionExpression) {
    String functionName = functionExpression.getFunction().getFunctionName().toUpperCase();
    String columnName =
        QueryRequestUtil.getLogicalColumnNameForSimpleColumnExpression(
            functionExpression.getFunction().getArguments(0));
    return String.join(":", functionName, columnName);
  }
}
