package org.hypertrace.core.query.service;

import static org.hypertrace.core.query.service.QueryRequestUtil.createStringArrayLiteralValueExpression;

import java.util.Arrays;
import java.util.List;
import org.hypertrace.core.query.service.api.AttributeExpression;
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

  public static Expression.Builder createSimpleAttributeExpression(String columnName) {
    return Expression.newBuilder()
        .setAttributeExpression(AttributeExpression.newBuilder().setAttributeId(columnName));
  }

  public static Expression.Builder createComplexAttributeExpression(
      String attributeId, String subPath) {
    return Expression.newBuilder()
        .setAttributeExpression(
            AttributeExpression.newBuilder().setAttributeId(attributeId).setSubpath(subPath));
  }

  public static Expression createAliasedColumnExpression(String columnName, String alias) {
    return Expression.newBuilder()
        .setColumnIdentifier(
            ColumnIdentifier.newBuilder().setColumnName(columnName).setAlias(alias))
        .build();
  }

  public static Expression createCountByColumnSelection(String columnNames) {
    return createFunctionExpression("Count", createColumnExpression(columnNames).build());
  }

  public static Expression createCountByColumnSelectionWithSimpleAttribute(String columnNames) {
    return createFunctionExpression("Count", createSimpleAttributeExpression(columnNames).build());
  }

  public static Expression createTimeColumnGroupByExpression(String timeColumn, String period) {
    return createFunctionExpression(
        "dateTimeConvert",
        createColumnExpression(timeColumn).build(),
        createStringLiteralValueExpression("1:MILLISECONDS:EPOCH"),
        createStringLiteralValueExpression("1:MILLISECONDS:EPOCH"),
        createStringLiteralValueExpression(period));
  }

  public static Expression createFunctionExpression(
      String functionName, Expression... expressions) {
    return Expression.newBuilder()
        .setFunction(
            Function.newBuilder()
                .setFunctionName(functionName)
                .addAllArguments(Arrays.asList(expressions)))
        .build();
  }

  public static Expression.Builder createAliasedFunctionExpression(
      String functionName, String columnNameArg, String alias) {
    return Expression.newBuilder()
        .setFunction(
            Function.newBuilder()
                .setAlias(alias)
                .setFunctionName(functionName)
                .addArguments(createColumnExpression(columnNameArg)));
  }

  public static Expression.Builder createAliasedFunctionExpressionWithSimpleAttribute(
      String functionName, String columnNameArg, String alias) {
    return Expression.newBuilder()
        .setFunction(
            Function.newBuilder()
                .setAlias(alias)
                .setFunctionName(functionName)
                .addArguments(createSimpleAttributeExpression(columnNameArg)));
  }

  public static Expression createAliasedFunctionExpression(
      String functionName, String alias, Expression... expressions) {
    return Expression.newBuilder()
        .setFunction(
            Function.newBuilder()
                .setFunctionName(functionName)
                .setAlias(alias)
                .addAllArguments(Arrays.asList(expressions)))
        .build();
  }

  public static Filter.Builder createCompositeFilter(Operator operator, Filter... childFilters) {
    return Filter.newBuilder().setOperator(operator).addAllChildFilter(Arrays.asList(childFilters));
  }

  public static Filter createInFilter(String column, List<String> values) {
    return createFilter(column, Operator.IN, createStringArrayLiteralValueExpression(values));
  }

  public static Filter createNotEqualsFilter(String column, String value) {
    return createFilter(column, Operator.NEQ, createStringLiteralValueExpression(value));
  }

  public static Filter createEqualsFilter(String column, String value) {
    return createFilter(column, Operator.EQ, createStringLiteralValueExpression(value));
  }

  public static Filter createEqualsFilter(String column, long value) {
    return createFilter(column, Operator.EQ, createLongLiteralValueExpression(value));
  }

  public static Filter createEqualsFilter(String column, int value) {
    return createFilter(column, Operator.EQ, createIntLiteralValueExpression(value));
  }

  public static Filter createEqualsFilter(String column, float value) {
    return createFilter(column, Operator.EQ, createFloatLiteralValueExpression(value));
  }

  public static Filter createEqualsFilter(String column, boolean value) {
    return createFilter(column, Operator.EQ, createBoolLiteralValueExpression(value));
  }

  public static Filter createEqualsFilter(String column, double value) {
    return createFilter(column, Operator.EQ, createDoubleLiteralValueExpression(value));
  }

  public static Filter createNullStringFilter(String columnName, Operator op) {
    return createFilter(columnName, op, createNullStringLiteralValueExpression());
  }

  public static Filter createTimestampFilter(String columnName, Operator op, long value) {
    return createFilter(columnName, op, createTimestampLiteralValueExpression(value));
  }

  public static Filter createTimeFilter(String columnName, Operator op, long value) {
    return createFilter(columnName, op, createLongLiteralValueExpression(value));
  }

  public static Filter createTimeFilterWithSimpleAttribute(
      String columnName, Operator op, long value) {
    return createSimpleAttributeFilter(columnName, op, createLongLiteralValueExpression(value));
  }

  public static Filter createFilter(String column, Operator operator, Expression expression) {
    return createFilter(createColumnExpression(column).build(), operator, expression);
  }

  public static Filter createSimpleAttributeFilter(
      String column, Operator operator, Expression expression) {
    return createFilter(createSimpleAttributeExpression(column).build(), operator, expression);
  }

  public static Filter createFilter(
      Expression columnExpression, Operator operator, Expression expression) {
    return Filter.newBuilder()
        .setLhs(columnExpression)
        .setOperator(operator)
        .setRhs(expression)
        .build();
  }

  public static Expression createStringLiteralValueExpression(String value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setString(value).setValueType(ValueType.STRING)))
        .build();
  }

  public static Expression createLongLiteralValueExpression(long value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setLong(value).setValueType(ValueType.LONG)))
        .build();
  }

  public static Expression createTimestampLiteralValueExpression(long value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setTimestamp(value).setValueType(ValueType.TIMESTAMP)))
        .build();
  }

  public static Expression createDoubleLiteralValueExpression(double value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setDouble(value).setValueType(ValueType.DOUBLE)))
        .build();
  }

  public static Expression createFloatLiteralValueExpression(float value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setFloat(value).setValueType(ValueType.FLOAT)))
        .build();
  }

  public static Expression createBoolLiteralValueExpression(boolean value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setBoolean(value).setValueType(ValueType.BOOL)))
        .build();
  }

  public static Expression createIntLiteralValueExpression(int value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setInt(value).setValueType(ValueType.INT)))
        .build();
  }

  public static Expression createNullStringLiteralValueExpression() {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.NULL_STRING)))
        .build();
  }

  public static Expression createNullNumberLiteralValueExpression() {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.NULL_NUMBER)))
        .build();
  }

  public static OrderByExpression.Builder createOrderByExpression(
      Expression.Builder expression, SortOrder sortOrder) {
    return OrderByExpression.newBuilder().setExpression(expression).setOrder(sortOrder);
  }
}
