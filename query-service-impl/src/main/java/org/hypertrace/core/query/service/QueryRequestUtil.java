package org.hypertrace.core.query.service;

import static org.hypertrace.core.query.service.api.Expression.ValueCase.ATTRIBUTE_EXPRESSION;

import java.util.Optional;
import org.hypertrace.core.query.service.api.AttributeExpression;
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

  public static Filter createNotContainsKeyFilter(String column, String value) {
    return Filter.newBuilder()
        .setOperator(Operator.NOT_CONTAINS_KEY)
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

  public static Optional<String> getLogicalColumnName(Expression expression) {
    switch (expression.getValueCase()) {
      case COLUMNIDENTIFIER:
        return Optional.of(expression.getColumnIdentifier().getColumnName());
      case ATTRIBUTE_EXPRESSION:
        return Optional.of(expression.getAttributeExpression().getAttributeId());
      default:
        return Optional.empty();
    }
  }

  public static Filter createLongFilter(String columnName, Operator op, long value) {
    return createFilter(columnName, op, createLongLiteralExpression(value));
  }

  public static Filter createFilter(String columnName, Operator op, Expression value) {
    return createFilter(createAttributeExpression(columnName), op, value);
  }

  public static Expression createAttributeExpression(String attributeId) {
    return Expression.newBuilder()
        .setAttributeExpression(AttributeExpression.newBuilder().setAttributeId(attributeId))
        .build();
  }

  public static Filter createFilter(Expression columnExpression, Operator op, Expression value) {
    return Filter.newBuilder().setLhs(columnExpression).setOperator(op).setRhs(value).build();
  }

  public static Optional<String> getAlias(Expression expression) {
    switch (expression.getValueCase()) {
      case COLUMNIDENTIFIER:
        return Optional.of(
            expression.getColumnIdentifier().getAlias().isBlank()
                ? getLogicalColumnName(expression).get()
                : expression.getColumnIdentifier().getAlias());
      case ATTRIBUTE_EXPRESSION:
        return Optional.of(getAlias(expression.getAttributeExpression()));
      case FUNCTION:
        // todo: handle recursive functions max(rollup(time,50)
        // workaround is to use alias for now
        return Optional.of(
            expression.getFunction().getAlias().isBlank()
                ? expression.getFunction().getFunctionName()
                : expression.getFunction().getAlias());
      default:
        return Optional.empty();
    }
  }

  public static String getAlias(AttributeExpression attributeExpression) {
    return attributeExpression.getAlias().isBlank()
        ? attributeExpression.getAttributeId()
        : attributeExpression.getAlias();
  }
}
