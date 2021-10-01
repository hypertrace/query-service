package org.hypertrace.core.query.service;

import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
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
}
