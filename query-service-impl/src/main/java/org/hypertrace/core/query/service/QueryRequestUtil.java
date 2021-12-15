package org.hypertrace.core.query.service;

import static org.hypertrace.core.query.service.api.Expression.ValueCase.ATTRIBUTE_EXPRESSION;
import static org.hypertrace.core.query.service.api.Expression.ValueCase.COLUMNIDENTIFIER;

import org.hypertrace.core.query.service.api.AttributeExpression;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Expression.ValueCase;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;

/**
 * Utility methods to easily create {@link org.hypertrace.core.query.service.api.QueryRequest} its
 * selections and filters.
 */
public class QueryRequestUtil {

  public static Expression createColumnExpression(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName))
        .build();
  }

  public static Expression createStringLiteralExpression(String value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(
                    Value.newBuilder()
                        .setValueType(ValueType.STRING)
                        .setString(String.valueOf(value))))
        .build();
  }

  public static Expression createLongLiteralExpression(long value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.LONG).setLong(value)))
        .build();
  }

  public static Expression createDoubleLiteralExpression(double value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(value)))
        .build();
  }

  public static Expression createBooleanLiteralExpression(boolean value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.BOOL).setBoolean(value)))
        .build();
  }

  public static Expression createNullStringLiteralExpression() {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.NULL_STRING)))
        .build();
  }

  public static Expression createNullNumberLiteralExpression() {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.NULL_NUMBER)))
        .build();
  }

  public static Filter createContainsKeyFilter(AttributeExpression complexAttributeExpression) {
    return createContainsKeyFilter(
        complexAttributeExpression.getAttributeId(), complexAttributeExpression.getSubpath());
  }

  public static Filter createContainsKeyFilter(String column, String value) {
    return Filter.newBuilder()
        .setOperator(Operator.CONTAINS_KEY)
        .setLhs(createSimpleAttributeExpression(column))
        .setRhs(createStringLiteralValueExpression(value))
        .build();
  }

  public static Expression createStringLiteralValueExpression(String value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setString(value).setValueType(ValueType.STRING)))
        .build();
  }

  public static Expression.Builder createSimpleAttributeExpression(String columnName) {
    return Expression.newBuilder()
        .setAttributeExpression(AttributeExpression.newBuilder().setAttributeId(columnName));
  }

  // attribute expression with sub path is always a map attribute
  public static boolean isAttributeExpressionWithSubpath(Expression expression) {
    return expression.getValueCase() == ATTRIBUTE_EXPRESSION
        && expression.getAttributeExpression().hasSubpath();
  }

  public static boolean isSimpleAttributeExpression(Expression expression) {
    switch (expression.getValueCase()) {
      case COLUMNIDENTIFIER:
        return true;
      case ATTRIBUTE_EXPRESSION:
        return !expression.getAttributeExpression().hasSubpath();
      default:
        return false;
    }
  }

  public static boolean isDateTimeFunction(Expression expression) {
    return expression.getValueCase() == ValueCase.FUNCTION
        && expression.getFunction().getFunctionName().equals("dateTimeConvert");
  }

  public static String getLogicalColumnName(Expression expression) {
    switch (expression.getValueCase()) {
      case COLUMNIDENTIFIER:
        return expression.getColumnIdentifier().getColumnName();
      case ATTRIBUTE_EXPRESSION:
        return expression.getAttributeExpression().getAttributeId();
      default:
        throw new IllegalArgumentException(
            "Supports "
                + ATTRIBUTE_EXPRESSION
                + " and "
                + COLUMNIDENTIFIER
                + " expression type only");
    }
  }
}
