package org.hypertrace.core.query.service.postgres.converters;

import static org.hypertrace.core.query.service.QueryRequestUtil.getLogicalColumnName;
import static org.hypertrace.core.query.service.QueryRequestUtil.isAttributeExpressionWithSubpath;
import static org.hypertrace.core.query.service.QueryRequestUtil.isSimpleAttributeExpression;
import static org.hypertrace.core.query.service.api.Expression.ValueCase.LITERAL;
import static org.hypertrace.core.query.service.postgres.converters.ColumnRequestContext.QueryPart.FILTER;
import static org.hypertrace.core.query.service.postgres.converters.ColumnRequestContext.QueryPart.GROUP_BY;
import static org.hypertrace.core.query.service.postgres.converters.ColumnRequestContext.QueryPart.ORDER_BY;
import static org.hypertrace.core.query.service.postgres.converters.ColumnRequestContext.QueryPart.SELECT;
import static org.hypertrace.core.query.service.postgres.converters.ColumnRequestContext.createColumnRequestContext;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.OrderByExpression;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;
import org.hypertrace.core.query.service.postgres.Params.Builder;
import org.hypertrace.core.query.service.postgres.TableDefinition;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Converts {@link QueryRequest} to Postgres SQL query */
class DefaultColumnRequestConverter implements ColumnRequestConverter {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultColumnRequestConverter.class);

  private static final String QUESTION_MARK = "?";
  private static final String REGEX_OPERATOR = "LIKE";
  private static final int MAP_KEY_INDEX = 0;
  private static final int MAP_VALUE_INDEX = 1;

  private final TableDefinition tableDefinition;
  private final PostgresFunctionConverter functionConverter;

  DefaultColumnRequestConverter(
      TableDefinition tableDefinition, PostgresFunctionConverter functionConverter) {
    this.tableDefinition = tableDefinition;
    this.functionConverter = functionConverter;
  }

  @Override
  public String convertSelectClause(
      Expression expression, Builder paramsBuilder, ExecutionContext executionContext) {
    return convertExpressionToString(
        expression, paramsBuilder, executionContext, createColumnRequestContext(SELECT));
  }

  @Override
  public String convertFilterClause(
      Filter filter, Builder paramsBuilder, ExecutionContext executionContext) {
    return convertFilterToString(
        filter, paramsBuilder, executionContext, createColumnRequestContext(FILTER));
  }

  @Override
  public String convertGroupByClause(
      Expression expression, Builder paramsBuilder, ExecutionContext executionContext) {
    return convertExpressionToString(
        expression, paramsBuilder, executionContext, createColumnRequestContext(GROUP_BY));
  }

  @Override
  public String convertOrderByClause(
      Expression expression, Builder paramsBuilder, ExecutionContext executionContext) {
    return convertExpressionToString(
        expression, paramsBuilder, executionContext, createColumnRequestContext(ORDER_BY));
  }

  private String convertFilterToString(
      Filter filter,
      Builder paramsBuilder,
      ExecutionContext executionContext,
      ColumnRequestContext context) {
    StringBuilder builder = new StringBuilder();
    String operator = convertOperatorToString(filter.getOperator());
    if (filter.getChildFilterCount() > 0) {
      String delim = "";
      builder.append("( ");
      for (Filter childFilter : filter.getChildFilterList()) {
        builder.append(delim);
        builder.append(
            convertFilterToString(childFilter, paramsBuilder, executionContext, context));
        builder.append(" ");
        delim = operator + " ";
      }
      builder.append(")");
    } else {
      String lhs =
          convertExpressionToString(filter.getLhs(), paramsBuilder, executionContext, context);
      switch (filter.getOperator()) {
        case LIKE:
          builder.append(lhs);
          builder.append(" ");
          builder.append(operator);
          builder.append(" ");
          builder.append(
              convertExpressionToString(filter.getRhs(), paramsBuilder, executionContext, context));
          break;
        case CONTAINS_KEY:
          builder.append(lhs);
          builder.append("->>");
          builder.append(
              convertLiteralToString(
                  convertMapKeyExpressionToLiterals(filter.getRhs()), paramsBuilder, context));
          builder.append(" IS NOT NULL");
          break;
        case NOT_CONTAINS_KEY:
          builder.append(lhs);
          builder.append("->>");
          builder.append(
              convertLiteralToString(
                  convertMapKeyExpressionToLiterals(filter.getRhs()), paramsBuilder, context));
          builder.append(" IS NULL");
          break;
        case CONTAINS_KEYVALUE:
          List<LiteralConstant> kvp = convertMapKeyValueExpressionToLiterals(filter.getRhs());
          builder.append(lhs);
          builder.append("->>");
          builder.append(convertLiteralToString(kvp.get(MAP_KEY_INDEX), paramsBuilder, context));
          builder.append(" = ");
          builder.append(convertLiteralToString(kvp.get(MAP_VALUE_INDEX), paramsBuilder, context));
          break;
        case CONTAINS_KEY_LIKE:
          builder.append(lhs);
          builder.append("::jsonb::text");
          builder.append(" ");
          builder.append(operator);
          builder.append(" ");
          builder.append(
              convertLiteralToString(
                  convertMapLikeExpressionToLiterals(filter.getRhs()), paramsBuilder, context));
          break;
        default:
          Expression rhs =
              handleValueConversionForLiteralExpression(filter.getLhs(), filter.getRhs());
          builder.append(lhs);
          builder.append(" ");
          builder.append(operator);
          builder.append(" ");
          builder.append(convertExpressionToString(rhs, paramsBuilder, executionContext, context));
      }
    }
    return builder.toString();
  }

  /**
   * Handles value conversion of a literal expression based on its associated column.
   *
   * @param lhs LHS expression with which literal is associated with
   * @param rhs RHS expression which needs value conversion if its a literal expression
   * @return newly created literal {@link Expression} of rhs if converted else the same one.
   */
  private Expression handleValueConversionForLiteralExpression(Expression lhs, Expression rhs) {
    if (!(isSimpleAttributeExpression(lhs) && rhs.getValueCase().equals(LITERAL))) {
      return rhs;
    }

    String lhsColumnName = getLogicalColumnName(lhs).orElseThrow(IllegalArgumentException::new);
    try {
      Value value =
          DestinationColumnValueConverter.INSTANCE.convert(
              rhs.getLiteral().getValue(), tableDefinition.getColumnType(lhsColumnName));
      return Expression.newBuilder()
          .setLiteral(LiteralConstant.newBuilder().setValue(value))
          .build();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid input:{ %s } for bytes column:{ %s }",
              rhs.getLiteral().getValue(), tableDefinition.getPhysicalColumnName(lhsColumnName)));
    }
  }

  private String convertOperatorToString(Operator operator) {
    switch (operator) {
      case AND:
        return "AND";
      case OR:
        return "OR";
      case NOT:
        return "NOT";
      case EQ:
        return "=";
      case NEQ:
      case NOT_CONTAINS_KEY:
        return "!=";
      case IN:
        return "IN";
      case NOT_IN:
        return "NOT IN";
      case GT:
        return ">";
      case LT:
        return "<";
      case GE:
        return ">=";
      case LE:
        return "<=";
      case LIKE:
      case CONTAINS_KEY_LIKE:
        return REGEX_OPERATOR;
      case CONTAINS_KEY:
      case CONTAINS_KEYVALUE:
        return "";
      case RANGE:
        throw new UnsupportedOperationException("RANGE NOT supported use >= and <=");
      case UNRECOGNIZED:
      default:
        throw new UnsupportedOperationException("Unknown operator:" + operator);
    }
  }

  private String convertExpressionToString(
      Expression expression,
      Builder paramsBuilder,
      ExecutionContext executionContext,
      ColumnRequestContext context) {
    switch (expression.getValueCase()) {
      case ATTRIBUTE_EXPRESSION:
        if (isAttributeExpressionWithSubpath(expression)) {
          String logicalColumnName =
              getLogicalColumnName(expression).orElseThrow(IllegalArgumentException::new);
          String columnName = tableDefinition.getPhysicalColumnName(logicalColumnName);
          String pathExpression = expression.getAttributeExpression().getSubpath();
          LiteralConstant pathExpressionLiteral =
              LiteralConstant.newBuilder()
                  .setValue(Value.newBuilder().setString(pathExpression).build())
                  .build();

          return columnName
              + "->>"
              + convertLiteralToString(pathExpressionLiteral, paramsBuilder, context);
        }
      case COLUMNIDENTIFIER:
        String logicalColumnName =
            getLogicalColumnName(expression).orElseThrow(IllegalArgumentException::new);
        String columnName = tableDefinition.getPhysicalColumnName(logicalColumnName);
        context.setColumnValueType(tableDefinition.getColumnType(logicalColumnName));
        if (context.isSelect() && context.isBytesColumnType()) {
          return String.format("encode(%s, 'hex')", columnName);
        } else if (context.isSelect() && context.isMapColumnType()) {
          return String.format("CAST(%s as text)", columnName);
        } else {
          return columnName;
        }
      case LITERAL:
        return convertLiteralToString(expression.getLiteral(), paramsBuilder, context);
      case FUNCTION:
        return this.functionConverter.convert(
            executionContext,
            expression.getFunction(),
            argExpression ->
                convertExpressionToString(argExpression, paramsBuilder, executionContext, context));
      case ORDERBY:
        OrderByExpression orderBy = expression.getOrderBy();
        return convertExpressionToString(
            orderBy.getExpression(), paramsBuilder, executionContext, context);
      case VALUE_NOT_SET:
        break;
    }
    return "";
  }

  private LiteralConstant convertMapKeyExpressionToLiterals(Expression expression) {
    List<String> literals = new ArrayList<>(1);
    if (expression.getValueCase() == LITERAL) {
      LiteralConstant value = expression.getLiteral();
      if (value.getValue().getValueType() == ValueType.STRING) {
        literals.add(value.getValue().getString());
      } else {
        throw new IllegalArgumentException("Unsupported arguments for CONTAINS_KEY operator");
      }
    }
    return getLiteralConstants(literals).get(0);
  }

  private List<LiteralConstant> convertMapKeyValueExpressionToLiterals(Expression expression) {
    List<String> literals = new ArrayList<>(2);
    if (expression.getValueCase() == LITERAL) {
      LiteralConstant value = expression.getLiteral();
      if (value.getValue().getValueType() == ValueType.STRING_ARRAY
          && value.getValue().getStringArrayCount() == 2) {
        literals.add(value.getValue().getStringArray(MAP_KEY_INDEX));
        literals.add(value.getValue().getStringArray(MAP_VALUE_INDEX));
      } else {
        throw new IllegalArgumentException("Unsupported arguments for CONTAINS_KEYVALUE  operator");
      }
    }
    return getLiteralConstants(literals);
  }

  private LiteralConstant convertMapLikeExpressionToLiterals(Expression expression) {
    List<String> literals = new ArrayList<>(1);
    if (expression.getValueCase() == LITERAL) {
      LiteralConstant value = expression.getLiteral();
      if (value.getValue().getValueType() == ValueType.STRING) {
        literals.add("%\"" + value.getValue().getString() + "\":%");
      } else {
        throw new IllegalArgumentException("Unsupported arguments for CONTAINS_KEY_LIKE operator");
      }
    }
    return getLiteralConstants(literals).get(0);
  }

  @NotNull
  private List<LiteralConstant> getLiteralConstants(List<String> literals) {
    return literals.stream()
        .map(
            literal ->
                LiteralConstant.newBuilder()
                    .setValue(Value.newBuilder().setString(literal))
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  /** TODO:Handle all types */
  private String convertLiteralToString(
      LiteralConstant literal, Builder paramsBuilder, ColumnRequestContext context) {
    Value value = literal.getValue();
    boolean isEmpty = false;
    String ret = null;
    switch (value.getValueType()) {
      case STRING_ARRAY:
        StringBuilder builder = new StringBuilder("(");
        String delim = "";
        for (String item : value.getStringArrayList()) {
          builder.append(delim);
          builder.append(QUESTION_MARK);
          paramsBuilder.addStringParam(item);
          delim = ", ";
        }
        builder.append(")");
        ret = builder.toString();
        break;
      case BYTES_ARRAY:
        builder = new StringBuilder("(");
        delim = "";
        for (ByteString item : value.getBytesArrayList()) {
          builder.append(delim);
          builder.append(QUESTION_MARK);
          paramsBuilder.addByteStringParam(item);
          delim = ", ";
        }
        builder.append(")");
        ret = builder.toString();
        break;
      case STRING:
        ret = QUESTION_MARK;
        paramsBuilder.addStringParam(value.getString());
        break;
      case LONG:
        ret = QUESTION_MARK;
        paramsBuilder.addLongParam(value.getLong());
        break;
      case INT:
        ret = QUESTION_MARK;
        paramsBuilder.addIntegerParam(value.getInt());
        break;
      case FLOAT:
        ret = QUESTION_MARK;
        paramsBuilder.addFloatParam(value.getFloat());
        break;
      case DOUBLE:
        ret = QUESTION_MARK;
        paramsBuilder.addDoubleParam(value.getDouble());
        break;
      case BYTES:
        ret = QUESTION_MARK;
        paramsBuilder.addByteStringParam(value.getBytes());
        isEmpty = value.getBytes().isEmpty();
        break;
      case BOOL:
        ret = QUESTION_MARK;
        paramsBuilder.addStringParam(String.valueOf(value.getBoolean()));
        break;
      case TIMESTAMP:
        ret = QUESTION_MARK;
        paramsBuilder.addLongParam(value.getTimestamp());
        break;
      case NULL_NUMBER:
        ret = QUESTION_MARK;
        paramsBuilder.addIntegerParam(0);
        break;
      case NULL_STRING:
        ret = QUESTION_MARK;
        paramsBuilder.addStringParam("null");
        break;
      case LONG_ARRAY:
      case INT_ARRAY:
      case FLOAT_ARRAY:
      case DOUBLE_ARRAY:
      case BOOLEAN_ARRAY:
      case UNRECOGNIZED:
        break;
    }
    if (ret != null && context.isBytesColumnType() && !isEmpty) {
      ret = ret.replace("?", "decode(?, 'hex')");
    }
    return ret;
  }
}