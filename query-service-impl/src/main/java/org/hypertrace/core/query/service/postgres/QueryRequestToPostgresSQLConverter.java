package org.hypertrace.core.query.service.postgres;

import static org.hypertrace.core.query.service.QueryRequestUtil.getLogicalColumnName;
import static org.hypertrace.core.query.service.QueryRequestUtil.isAttributeExpressionWithSubpath;
import static org.hypertrace.core.query.service.QueryRequestUtil.isSimpleAttributeExpression;
import static org.hypertrace.core.query.service.api.Expression.ValueCase.LITERAL;

import com.google.protobuf.ByteString;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.OrderByExpression;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.SortOrder;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;
import org.hypertrace.core.query.service.postgres.Params.Builder;
import org.hypertrace.core.query.service.postgres.converters.DestinationColumnValueConverter;
import org.hypertrace.core.query.service.postgres.converters.PostgresFunctionConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Converts {@link QueryRequest} to Postgres SQL query */
class QueryRequestToPostgresSQLConverter {

  private static final Logger LOG =
      LoggerFactory.getLogger(QueryRequestToPostgresSQLConverter.class);

  private static final String QUESTION_MARK = "?";
  private static final String REGEX_OPERATOR = "LIKE";
  private static final String MAP_VALUE = "mapValue";

  private final TableDefinition tableDefinition;
  private final PostgresFunctionConverter functionConverter;

  QueryRequestToPostgresSQLConverter(
      TableDefinition tableDefinition, PostgresFunctionConverter functionConverter) {
    this.tableDefinition = tableDefinition;
    this.functionConverter = functionConverter;
  }

  Entry<String, Params> toSQL(
      ExecutionContext executionContext,
      QueryRequest request,
      LinkedHashSet<Expression> allSelections) {
    Context context = new Context();
    Builder paramsBuilder = Params.newBuilder();
    StringBuilder pqlBuilder = new StringBuilder("Select ");
    String delim = "";

    // Set the DISTINCT keyword if the request has set distinctSelections.
    if (request.getDistinctSelections()) {
      pqlBuilder.append("DISTINCT ");
    }

    context.setSelect(true);
    // allSelections contain all the various expressions in QueryRequest that we want selections on.
    // Group bys, selections and aggregations in that order. See RequestAnalyzer#analyze() to see
    // how it is created.
    for (Expression expr : allSelections) {
      pqlBuilder.append(delim);
      pqlBuilder.append(convertExpressionToString(expr, paramsBuilder, executionContext, context));
      delim = ", ";
    }

    pqlBuilder.append(" FROM public.\"").append(tableDefinition.getTableName()).append("\"");

    context.setSelect(false);

    // Add the tenantId filter
    pqlBuilder.append(" WHERE ").append(tableDefinition.getTenantIdColumn()).append(" = ?");
    paramsBuilder.addStringParam(executionContext.getTenantId());

    if (request.hasFilter()) {
      pqlBuilder.append(" AND ");
      String filterClause =
          convertFilterToString(request.getFilter(), paramsBuilder, executionContext, context);
      pqlBuilder.append(filterClause);
    }

    if (request.getGroupByCount() > 0) {
      pqlBuilder.append(" GROUP BY ");
      delim = "";
      for (Expression groupByExpression : request.getGroupByList()) {
        pqlBuilder.append(delim);
        pqlBuilder.append(
            convertExpressionToString(groupByExpression, paramsBuilder, executionContext, context));
        delim = ", ";
      }
    }
    if (!request.getOrderByList().isEmpty()) {
      pqlBuilder.append(" ORDER BY ");
      delim = "";
      for (OrderByExpression orderByExpression : request.getOrderByList()) {
        pqlBuilder.append(delim);
        pqlBuilder.append(
            convertExpressionToString(
                orderByExpression.getExpression(), paramsBuilder, executionContext, context));
        if (SortOrder.DESC.equals(orderByExpression.getOrder())) {
          pqlBuilder.append(" desc ");
        }
        delim = ", ";
      }
    }
    if (request.getLimit() > 0) {
      if (request.getOffset() > 0) {
        pqlBuilder
            .append(" limit ")
            .append(request.getOffset())
            .append(", ")
            .append(request.getLimit());
      } else {
        pqlBuilder.append(" limit ").append(request.getLimit());
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Converted QueryRequest to Postgres SQL: {}", pqlBuilder);
    }
    return new SimpleEntry<>(pqlBuilder.toString(), paramsBuilder.build());
  }

  private String convertFilterToString(
      Filter filter, Builder paramsBuilder, ExecutionContext executionContext, Context context) {
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
      Expression rhs;
      switch (filter.getOperator()) {
        case LIKE:
        case CONTAINS_KEY:
        case NOT_CONTAINS_KEY:
        case CONTAINS_KEYVALUE:
        case CONTAINS_KEY_LIKE:
          builder.append(
              convertExpressionToString(filter.getLhs(), paramsBuilder, executionContext, context));
          builder.append(" ");
          builder.append(operator);
          builder.append(" ");
          builder.append(
              convertExpressionToString(filter.getRhs(), paramsBuilder, executionContext, context));
          break;
        default:
          rhs = handleValueConversionForLiteralExpression(filter.getLhs(), filter.getRhs());
          builder.append(
              convertExpressionToString(filter.getLhs(), paramsBuilder, executionContext, context));
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
      case CONTAINS_KEY:
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
      case CONTAINS_KEYVALUE:
        return MAP_VALUE;
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
      Context context) {
    switch (expression.getValueCase()) {
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
      case ATTRIBUTE_EXPRESSION:
        if (isAttributeExpressionWithSubpath(expression)) {
          String pathExpression = expression.getAttributeExpression().getSubpath();
          LiteralConstant pathExpressionLiteral =
              LiteralConstant.newBuilder()
                  .setValue(Value.newBuilder().setString(pathExpression).build())
                  .build();

          return String.format(
              "%s(%s,%s,%s)",
              MAP_VALUE,
              "keyCol",
              convertLiteralToString(pathExpressionLiteral, paramsBuilder, context),
              "valCol");
        } else {
          String logColumnName =
              getLogicalColumnName(expression).orElseThrow(IllegalArgumentException::new);
          columnName = tableDefinition.getPhysicalColumnName(logColumnName);
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

  private List<LiteralConstant> convertExpressionToMapLiterals(Expression expression) {
    List<String> literals = new ArrayList<>(List.of("", ""));
    if (expression.getValueCase() == LITERAL) {
      LiteralConstant value = expression.getLiteral();
      if (value.getValue().getValueType() == ValueType.STRING_ARRAY
          && value.getValue().getStringArrayCount() == 2) {
        literals.set(0, value.getValue().getStringArray(0));
        literals.set(1, value.getValue().getStringArray(1));
      } else if (value.getValue().getValueType() == ValueType.STRING) {
        literals.set(0, value.getValue().getString());
      } else {
        throw new IllegalArgumentException(
            "Unsupported arguments for CONTAINS_KEY / CONTAINS_KEYVALUE / CONTAINS_KEY_LIKE operator");
      }
    }

    List<LiteralConstant> literalConstantList = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      literalConstantList.add(
          LiteralConstant.newBuilder()
              .setValue(Value.newBuilder().setString(literals.get(i)).build())
              .build());
    }

    return literalConstantList;
  }

  /** TODO:Handle all types */
  private String convertLiteralToString(
      LiteralConstant literal, Builder paramsBuilder, Context context) {
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

  class Context {
    private boolean select;
    private ValueType columnValueType;

    public void setSelect(boolean select) {
      this.select = select;
    }

    public void setColumnValueType(ValueType columnValueType) {
      this.columnValueType = columnValueType;
    }

    boolean isSelect() {
      return select;
    }

    boolean isBytesColumnType() {
      return columnValueType != null && columnValueType.equals(ValueType.BYTES);
    }

    boolean isMapColumnType() {
      return columnValueType != null && columnValueType.equals(ValueType.STRING_MAP);
    }
  }
}
