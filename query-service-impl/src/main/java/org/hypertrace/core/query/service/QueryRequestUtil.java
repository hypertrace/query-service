package org.hypertrace.core.query.service;

import java.util.List;
import org.hypertrace.core.query.service.api.AttributeExpression;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;
import org.hypertrace.core.query.service.pinot.ViewDefinition;

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
        .setLhs(createColumnExpression(column))
        .setRhs(createStringArrayLiteralValueExpression(List.of(value)))
        .build();
  }

  public static Expression createStringArrayLiteralValueExpression(List<String> values) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(
                    Value.newBuilder()
                        .addAllStringArray(values)
                        .setValueType(ValueType.STRING_ARRAY)))
        .build();
  }

  public static boolean isMapAttributeExpression(
      Expression expression, ViewDefinition viewDefinition) {
    switch (expression.getValueCase()) {
      case COLUMNIDENTIFIER:
        return isMapField(expression.getColumnIdentifier().getColumnName(), viewDefinition);
      case ATTRIBUTE_EXPRESSION:
        return expression.getAttributeExpression().hasSubpath()
            && isMapField(expression.getAttributeExpression().getAttributeId(), viewDefinition);
      default:
        return false;
    }
  }

  public static boolean isSimpleAttributeExpression(
      Expression expression, ViewDefinition viewDefinition) {
    switch (expression.getValueCase()) {
      case COLUMNIDENTIFIER:
        return true;
      case ATTRIBUTE_EXPRESSION:
        return !expression.getAttributeExpression().hasSubpath()
            || !isMapField(expression.getAttributeExpression().getAttributeId(), viewDefinition);
      default:
        return false;
    }
  }

  private static boolean isMapField(String columnName, ViewDefinition viewDefinition) {
    return viewDefinition.getColumnType(columnName) == ValueType.STRING_MAP;
  }
}
