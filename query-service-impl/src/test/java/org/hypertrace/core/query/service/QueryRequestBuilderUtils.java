package org.hypertrace.core.query.service;

import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.OrderByExpression;
import org.hypertrace.core.query.service.api.SortOrder;
import org.hypertrace.core.query.service.api.Value;

public class QueryRequestBuilderUtils {
  public static Expression.Builder createColumnExpression(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName));
  }

  public static Expression.Builder createFunctionExpression(
      String functionName, String columnNameArg, String alias) {
    return Expression.newBuilder()
        .setFunction(
            Function.newBuilder()
                .setAlias(alias)
                .setFunctionName(functionName)
                .addArguments(
                    Expression.newBuilder()
                        .setColumnIdentifier(
                            ColumnIdentifier.newBuilder().setColumnName(columnNameArg))));
  }

  public static OrderByExpression.Builder createOrderByExpression(
      Expression.Builder expression, SortOrder sortOrder) {
    return OrderByExpression.newBuilder().setExpression(expression).setOrder(sortOrder);
  }

  public static Filter createEqualsFilter(String column, String value) {
    return Filter.newBuilder().setLhs(createColumnExpression(column))
        .setOperator(Operator.EQ).setRhs(createStringLiteralValueExpression(value)).build();
  }

  public static Expression createStringLiteralValueExpression(String value) {
    return Expression.newBuilder().setLiteral(LiteralConstant.newBuilder().setValue(
        Value.newBuilder().setString(value))).build();
  }
}
