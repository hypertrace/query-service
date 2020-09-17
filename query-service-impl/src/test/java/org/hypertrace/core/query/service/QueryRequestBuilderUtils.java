package org.hypertrace.core.query.service;

import java.util.List;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.OrderByExpression;
import org.hypertrace.core.query.service.api.SortOrder;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;

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
    return createFilter(column, Operator.EQ, createStringLiteralValueExpression(value));
  }

  public static Filter createInFilter(String column, List<String> values) {
    return createFilter(column, Operator.IN, createStringArrayLiteralValueExpression(values));
  }

  public static Filter createEqualsFilter(String column, long value) {
    return createFilter(column, Operator.EQ, createLongLiteralValueExpression(value));
  }

  public static Filter createEqualsFilter(String column, int value) {
    return createFilter(column, Operator.EQ, createIntLiteralValueExpression(value));
  }

  public static Filter createFilter(String column, Operator operator, Expression expression) {
    return Filter.newBuilder().setLhs(createColumnExpression(column))
        .setOperator(operator).setRhs(expression).build();
  }

  public static Expression createStringLiteralValueExpression(String value) {
    return Expression.newBuilder().setLiteral(LiteralConstant.newBuilder().setValue(
        Value.newBuilder().setString(value).setValueType(ValueType.STRING))).build();
  }

  public static Expression createStringArrayLiteralValueExpression(List<String> values) {
    return Expression.newBuilder().setLiteral(LiteralConstant.newBuilder().setValue(
        Value.newBuilder().addAllStringArray(values).setValueType(ValueType.STRING_ARRAY))).build();
  }

  public static Expression createLongLiteralValueExpression(long value) {
    return Expression.newBuilder().setLiteral(LiteralConstant.newBuilder().setValue(
        Value.newBuilder().setLong(value).setValueType(ValueType.LONG))).build();
  }

  public static Expression createIntLiteralValueExpression(int value) {
    return Expression.newBuilder().setLiteral(LiteralConstant.newBuilder().setValue(
        Value.newBuilder().setInt(value).setValueType(ValueType.INT))).build();
  }
}
