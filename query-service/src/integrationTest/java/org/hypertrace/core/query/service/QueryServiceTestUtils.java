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
import org.hypertrace.core.query.service.api.ValueType;

public class QueryServiceTestUtils {

  public static Filter createFilter(
      String columnName, Operator op, ValueType valueType, Object valueObject) {
    ColumnIdentifier startTimeColumn =
        ColumnIdentifier.newBuilder().setColumnName(columnName).build();
    Expression lhs = Expression.newBuilder().setColumnIdentifier(startTimeColumn).build();

    Value value = Value.newBuilder().setValueType(valueType).buildPartial();
    switch (valueType) {
      case LONG:
        value = Value.newBuilder(value).setLong((long) valueObject).build();
        break;
      case INT:
        value = Value.newBuilder(value).setInt((int) valueObject).build();
        break;
      case STRING:
        value = Value.newBuilder(value).setString((String) valueObject).build();
    }
    LiteralConstant constant = LiteralConstant.newBuilder().setValue(value).build();
    Expression rhs = Expression.newBuilder().setLiteral(constant).build();
    return Filter.newBuilder().setLhs(lhs).setOperator(op).setRhs(rhs).build();
  }

  public static Expression createTimeColumnGroupByFunctionExpression(
      String timeColumn, String periodSecs) {
    return Expression.newBuilder()
        .setFunction(
            Function.newBuilder()
                .setFunctionName("dateTimeConvert")
                .addArguments(
                    Expression.newBuilder()
                        .setColumnIdentifier(
                            ColumnIdentifier.newBuilder().setColumnName(timeColumn)))
                .addArguments(
                    Expression.newBuilder()
                        .setLiteral(
                            LiteralConstant.newBuilder()
                                .setValue(Value.newBuilder().setString("1:MILLISECONDS:EPOCH"))))
                .addArguments(
                    Expression.newBuilder()
                        .setLiteral(
                            LiteralConstant.newBuilder()
                                .setValue(Value.newBuilder().setString("1:MILLISECONDS:EPOCH"))))
                .addArguments(
                    Expression.newBuilder()
                        .setLiteral(
                            LiteralConstant.newBuilder()
                                .setValue(Value.newBuilder().setString(periodSecs))))
                .build())
        .build();
  }

  public static Expression createColumnExpression(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName))
        .build();
  }

  public static Expression createFunctionExpression(String functionName, Expression columnNameArg) {
    return Expression.newBuilder()
        .setFunction(
            Function.newBuilder().setFunctionName(functionName).addArguments(columnNameArg))
        .build();
  }

  public static Expression createFunctionExpression(
      String functionName, Expression columnNameArg1, Expression columnNameArg2) {
    return Expression.newBuilder()
        .setFunction(
            Function.newBuilder()
                .setFunctionName(functionName)
                .addArguments(columnNameArg1)
                .addArguments(columnNameArg2))
        .build();
  }

  public static OrderByExpression createOrderByExpression(
      Expression expression, SortOrder sortOrder) {
    return OrderByExpression.newBuilder().setExpression(expression).setOrder(sortOrder).build();
  }

  public static Expression createStringLiteralValueExpression(String value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setString(value).setValueType(ValueType.STRING)))
        .build();
  }
}
