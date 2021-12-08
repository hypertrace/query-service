package org.hypertrace.core.query.service.prometheus;

import static org.hypertrace.core.query.service.QueryRequestUtil.getLogicalColumnNameForSimpleColumnExpression;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.function.Function;
import org.apache.commons.codec.binary.Hex;
import org.hypertrace.core.query.service.QueryRequestUtil;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.Value;

class FilterToPromqlConverter {

  /**
   * only `AND` operator in filter is allowed
   *
   * <p>rhs of leaf filter should be literal
   */
  void convertFilterToString(
      Filter filter,
      List<String> filterList,
      String timeFilterColumn,
      Function<Expression, String> expressionToColumnConverter) {
    if (filter.getChildFilterCount() > 0) {
      for (Filter childFilter : filter.getChildFilterList()) {
        convertFilterToString(
            childFilter, filterList, timeFilterColumn, expressionToColumnConverter);
      }
    } else {
      if (QueryRequestUtil.isSimpleColumnExpression(filter.getLhs())
          && timeFilterColumn.equals(
              getLogicalColumnNameForSimpleColumnExpression(filter.getLhs()))) {
        return;
      }
      StringBuilder builder = new StringBuilder();
      builder.append(expressionToColumnConverter.apply(filter.getLhs()));
      builder.append(convertOperatorToString(filter.getOperator()));
      builder.append(convertLiteralToString(filter.getRhs().getLiteral()));
      filterList.add(builder.toString());
    }
  }

  private String convertOperatorToString(Operator operator) {
    switch (operator) {
      case IN:
      case EQ:
        return "=";
      case NEQ:
        return "!=";
      case LIKE:
        return "=~";
      default:
        throw new RuntimeException(
            String.format("Equivalent %s operator not supported in promql", operator));
    }
  }

  private String convertLiteralToString(LiteralConstant literal) {
    Value value = literal.getValue();
    String ret = null;
    switch (value.getValueType()) {
      case STRING_ARRAY:
        StringBuilder builder = new StringBuilder("\"");
        for (String item : value.getStringArrayList()) {
          if (builder.length() > 1) {
            builder.append("|");
          }
          builder.append(item);
        }
        builder.append("\"");
        ret = builder.toString();
        break;
      case BYTES_ARRAY:
        builder = new StringBuilder("\"");
        for (ByteString item : value.getBytesArrayList()) {
          if (builder.length() > 1) {
            builder.append("|");
          }
          builder.append(Hex.encodeHexString(item.toByteArray()));
        }
        builder.append("\"");
        ret = builder.toString();
        break;
      case STRING:
        ret = "\"" + value.getString() + "\"";
        break;
      case LONG:
        ret = "\"" + value.getLong() + "\"";
        break;
      case INT:
        ret = "\"" + value.getInt() + "\"";
        break;
      case FLOAT:
        ret = "\"" + value.getFloat() + "\"";
        break;
      case DOUBLE:
        ret = "\"" + value.getDouble() + "\"";
        break;
      case BYTES:
        ret = "\"" + Hex.encodeHexString(value.getBytes().toByteArray()) + "\"";
        break;
      case BOOL:
        ret = "\"" + value.getBoolean() + "\"";
        break;
      case TIMESTAMP:
        ret = "\"" + value.getTimestamp() + "\"";
        break;
      case NULL_NUMBER:
        ret = "0";
        break;
      case NULL_STRING:
        ret = "null";
        break;
      case LONG_ARRAY:
      case INT_ARRAY:
      case FLOAT_ARRAY:
      case DOUBLE_ARRAY:
      case BOOLEAN_ARRAY:
      case UNRECOGNIZED:
        break;
    }
    return ret;
  }
}
