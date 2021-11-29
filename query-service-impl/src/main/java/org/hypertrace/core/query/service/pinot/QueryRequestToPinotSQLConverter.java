package org.hypertrace.core.query.service.pinot;

import static org.hypertrace.core.query.service.api.Expression.ValueCase.ATTRIBUTE_EXPRESSION;
import static org.hypertrace.core.query.service.api.Expression.ValueCase.COLUMNIDENTIFIER;
import static org.hypertrace.core.query.service.api.Expression.ValueCase.LITERAL;

import com.google.common.base.Joiner;
import com.google.protobuf.ByteString;
import java.util.AbstractMap.SimpleEntry;
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
import org.hypertrace.core.query.service.pinot.Params.Builder;
import org.hypertrace.core.query.service.pinot.converters.DestinationColumnValueConverter;
import org.hypertrace.core.query.service.pinot.converters.PinotFunctionConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Converts {@link QueryRequest} to Pinot SQL query */
class QueryRequestToPinotSQLConverter {

  private static final Logger LOG = LoggerFactory.getLogger(QueryRequestToPinotSQLConverter.class);

  private static final String QUESTION_MARK = "?";
  private static final String REGEX_OPERATOR = "REGEXP_LIKE";
  private static final String MAP_VALUE = "mapValue";
  private static final int MAP_KEY_INDEX = 0;
  private static final int MAP_VALUE_INDEX = 1;

  private static final List<Operator> SUPPORTED_OPERATORS_FOR_MAP_ATTRIBUTES_WITH_SUBPATH =
      List.of(Operator.EQ, Operator.NEQ, Operator.GT, Operator.GE, Operator.LT, Operator.LE);

  private final ViewDefinition viewDefinition;
  private final PinotFunctionConverter functionConverter;
  private final Joiner joiner = Joiner.on(", ").skipNulls();

  QueryRequestToPinotSQLConverter(
      ViewDefinition viewDefinition, PinotFunctionConverter functionConverter) {
    this.viewDefinition = viewDefinition;
    this.functionConverter = functionConverter;
  }

  Entry<String, Params> toSQL(
      ExecutionContext executionContext,
      QueryRequest request,
      LinkedHashSet<Expression> allSelections) {
    Params.Builder paramsBuilder = Params.newBuilder();
    StringBuilder pqlBuilder = new StringBuilder("Select ");
    String delim = "";

    // Set the DISTINCT keyword if the request has set distinctSelections.
    if (request.getDistinctSelections()) {
      pqlBuilder.append("DISTINCT ");
    }

    // allSelections contain all the various expressions in QueryRequest that we want selections on.
    // Group bys, selections and aggregations in that order. See RequestAnalyzer#analyze() to see
    // how it is created.
    for (Expression expr : allSelections) {
      pqlBuilder.append(delim);
      pqlBuilder.append(
          isGroupingSelectionForMapAttribute(expr, request.getGroupByList())
              ? convertExpression2StringForMapAttribute(expr, paramsBuilder, executionContext)
              : convertExpression2String(expr, paramsBuilder, executionContext));
      delim = ", ";
    }

    pqlBuilder.append(" FROM ").append(viewDefinition.getViewName());

    // Add the tenantId filter
    pqlBuilder.append(" WHERE ").append(viewDefinition.getTenantIdColumn()).append(" = ?");
    paramsBuilder.addStringParam(executionContext.getTenantId());

    if (request.hasFilter()) {
      pqlBuilder.append(" AND ");
      String filterClause =
          convertFilter2String(request.getFilter(), paramsBuilder, executionContext);
      pqlBuilder.append(filterClause);
    }

    if (request.getGroupByCount() > 0) {
      pqlBuilder.append(" GROUP BY ");
      delim = "";
      for (Expression groupByExpression : request.getGroupByList()) {
        pqlBuilder.append(delim);
        pqlBuilder.append(
            convertExpression2StringForMapAttribute(
                groupByExpression, paramsBuilder, executionContext));
        delim = ", ";
      }
    }
    if (!request.getOrderByList().isEmpty()) {
      pqlBuilder.append(" ORDER BY ");
      delim = "";
      for (OrderByExpression orderByExpression : request.getOrderByList()) {
        pqlBuilder.append(delim);
        String orderBy =
            convertExpression2StringForMapAttribute(
                orderByExpression.getExpression(), paramsBuilder, executionContext);
        pqlBuilder.append(orderBy);
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
      LOG.debug("Converted QueryRequest to Pinot SQL: {}", pqlBuilder);
    }
    return new SimpleEntry<>(pqlBuilder.toString(), paramsBuilder.build());
  }

  private String convertFilter2String(
      Filter filter, Builder paramsBuilder, ExecutionContext executionContext) {
    StringBuilder builder = new StringBuilder();
    String operator = convertOperator2String(filter.getOperator());
    if (filter.getChildFilterCount() > 0) {
      String delim = "";
      builder.append("( ");
      for (Filter childFilter : filter.getChildFilterList()) {
        builder.append(delim);
        builder.append(convertFilter2String(childFilter, paramsBuilder, executionContext));
        builder.append(" ");
        delim = operator + " ";
      }
      builder.append(")");
    } else {
      switch (filter.getOperator()) {
        case LIKE:
          // The like operation in PQL looks like `regexp_like(lhs, rhs)`
          Expression rhs =
              handleValueConversionForLiteralExpression(filter.getLhs(), filter.getRhs());
          builder.append(operator);
          builder.append("(");
          builder.append(
              convertExpression2String(filter.getLhs(), paramsBuilder, executionContext));
          builder.append(",");
          builder.append(convertExpression2String(rhs, paramsBuilder, executionContext));
          builder.append(")");
          break;
        case CONTAINS_KEY:
          LiteralConstant[] kvp = convertExpressionToMapLiterals(filter.getRhs());
          builder.append(convertExpressionToMapKeyColumn(filter.getLhs()));
          builder.append(" = ");
          builder.append(convertLiteralToString(kvp[MAP_KEY_INDEX], paramsBuilder));
          break;
        case CONTAINS_KEYVALUE:
          kvp = convertExpressionToMapLiterals(filter.getRhs());
          String keyCol = convertExpressionToMapKeyColumn(filter.getLhs());
          String valCol = convertExpressionToMapValueColumn(filter.getLhs());
          builder.append(keyCol);
          builder.append(" = ");
          builder.append(convertLiteralToString(kvp[MAP_KEY_INDEX], paramsBuilder));
          builder.append(" AND ");
          builder.append(valCol);
          builder.append(" = ");
          builder.append(convertLiteralToString(kvp[MAP_VALUE_INDEX], paramsBuilder));
          builder.append(" AND ");
          builder.append(MAP_VALUE);
          builder.append("(");
          builder.append(keyCol);
          builder.append(",");
          builder.append(convertLiteralToString(kvp[MAP_KEY_INDEX], paramsBuilder));
          builder.append(",");
          builder.append(valCol);
          builder.append(") = ");
          builder.append(convertLiteralToString(kvp[MAP_VALUE_INDEX], paramsBuilder));
          break;
        default:
          if (isComplexAttribute(filter.getLhs())) {
            builder.append(handleFilterForComplexAttribute(filter, paramsBuilder));
          } else {
            rhs = handleValueConversionForLiteralExpression(filter.getLhs(), filter.getRhs());
            builder.append(
                convertExpression2String(filter.getLhs(), paramsBuilder, executionContext));
            builder.append(" ");
            builder.append(operator);
            builder.append(" ");
            builder.append(convertExpression2String(rhs, paramsBuilder, executionContext));
          }
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
    if (!((lhs.getValueCase().equals(COLUMNIDENTIFIER)
            || (lhs.getValueCase().equals(ATTRIBUTE_EXPRESSION) && !isComplexAttribute(lhs)))
        && rhs.getValueCase().equals(LITERAL))) {
      return rhs;
    }

    String lhsColumnName = getLogicalColumnName(lhs);
    try {
      Value value =
          DestinationColumnValueConverter.INSTANCE.convert(
              rhs.getLiteral().getValue(), viewDefinition.getColumnType(lhsColumnName));
      return Expression.newBuilder()
          .setLiteral(LiteralConstant.newBuilder().setValue(value))
          .build();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid input:{ %s } for bytes column:{ %s }",
              rhs.getLiteral().getValue(),
              viewDefinition.getPhysicalColumnNames(lhsColumnName).get(0)));
    }
  }

  private String convertOperator2String(Operator operator) {
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
        return REGEX_OPERATOR;
      case CONTAINS_KEY:
      case CONTAINS_KEYVALUE:
        return MAP_VALUE;
      case RANGE:
        throw new UnsupportedOperationException("RANGE NOT supported use >= and <=");
      case UNRECOGNIZED:
      default:
        throw new UnsupportedOperationException("Unknown operator:" + operator);
    }
  }

  private LiteralConstant[] getKeyValuePairForComplexAttribute(Expression lhs, Expression rhs) {
    String pathExpression = lhs.getAttributeExpression().getSubpath();
    LiteralConstant[] kvp = convertExpressionToMapLiterals(rhs);
    kvp[0] =
        LiteralConstant.newBuilder()
            .setValue(Value.newBuilder().setString(pathExpression).build())
            .build();
    return kvp;
  }

  private String handleFilterForComplexAttribute(Filter filter, Builder paramsBuilder) {
    LiteralConstant[] kvp = getKeyValuePairForComplexAttribute(filter.getLhs(), filter.getRhs());
    String keyCol = convertExpressionToMapKeyColumn(filter.getLhs());
    String valCol = convertExpressionToMapValueColumn(filter.getLhs());

    if (!SUPPORTED_OPERATORS_FOR_MAP_ATTRIBUTES_WITH_SUBPATH.contains(filter.getOperator())) {
      throw new UnsupportedOperationException(
          "Unknown operator for map attributes:" + filter.getOperator());
    }

    StringBuilder builder = new StringBuilder();
    builder.append(keyCol);
    builder.append(" = ");
    builder.append(convertLiteralToString(kvp[MAP_KEY_INDEX], paramsBuilder));
    builder.append(" AND ");
    builder.append(valCol);
    builder.append(" ");
    builder.append(convertOperator2String(filter.getOperator()));
    builder.append(" ");
    builder.append(convertLiteralToString(kvp[MAP_VALUE_INDEX], paramsBuilder));
    builder.append(" AND ");
    builder.append(MAP_VALUE);
    builder.append("(");
    builder.append(keyCol);
    builder.append(",");
    builder.append(convertLiteralToString(kvp[MAP_KEY_INDEX], paramsBuilder));
    builder.append(",");
    builder.append(valCol);
    builder.append(")");
    builder.append(" ");
    builder.append(convertOperator2String(filter.getOperator()));
    builder.append(" ");
    builder.append(convertLiteralToString(kvp[MAP_VALUE_INDEX], paramsBuilder));
    return builder.toString();
  }

  private boolean isGroupingSelectionForMapAttribute(
      Expression expression, List<Expression> groupByList) {
    if (expression.getValueCase() == ATTRIBUTE_EXPRESSION && isComplexAttribute(expression)) {
      String attributeId = expression.getAttributeExpression().getAttributeId();
      String subPath = expression.getAttributeExpression().getSubpath();
      for (Expression groupByExpression : groupByList) {
        if (groupByExpression.getValueCase() == ATTRIBUTE_EXPRESSION
            && isComplexAttribute(groupByExpression)
            && groupByExpression.getAttributeExpression().getSubpath().equals(subPath)
            && groupByExpression.getAttributeExpression().getAttributeId().equals(attributeId)) {
          return true;
        }
      }
    }
    return false;
  }

  private String convertExpression2StringForMapAttribute(
      Expression expression, Builder paramsBuilder, ExecutionContext executionContext) {
    if (expression.getValueCase() == ATTRIBUTE_EXPRESSION && isComplexAttribute(expression)) {
      String keyCol = convertExpressionToMapKeyColumn(expression);
      String valCol = convertExpressionToMapValueColumn(expression);
      String pathExpression = expression.getAttributeExpression().getSubpath();

      StringBuilder builder = new StringBuilder();
      builder.append(MAP_VALUE);
      builder.append("(");
      builder.append(keyCol);
      builder.append(",");
      builder.append(
          convertLiteralToString(
              LiteralConstant.newBuilder()
                  .setValue(Value.newBuilder().setString(pathExpression).build())
                  .build(),
              paramsBuilder));
      builder.append(",");
      builder.append(valCol);
      builder.append(")");
      return builder.toString();
    } else {
      return convertExpression2String(expression, paramsBuilder, executionContext);
    }
  }

  private String convertExpression2String(
      Expression expression, Builder paramsBuilder, ExecutionContext executionContext) {
    switch (expression.getValueCase()) {
      case COLUMNIDENTIFIER:
      case ATTRIBUTE_EXPRESSION:
        // this takes care of the Map Type where it's split into 2 columns
        List<String> columnNames =
            viewDefinition.getPhysicalColumnNames(getLogicalColumnName(expression));
        return joiner.join(columnNames);
      case LITERAL:
        return convertLiteralToString(expression.getLiteral(), paramsBuilder);
      case FUNCTION:
        return this.functionConverter.convert(
            executionContext,
            expression.getFunction(),
            argExpression ->
                convertExpression2String(argExpression, paramsBuilder, executionContext));
      case ORDERBY:
        OrderByExpression orderBy = expression.getOrderBy();
        return convertExpression2String(orderBy.getExpression(), paramsBuilder, executionContext);
      case VALUE_NOT_SET:
        break;
    }
    return "";
  }

  private String convertExpressionToMapKeyColumn(Expression expression) {
    if ((expression.getValueCase() == COLUMNIDENTIFIER)
        || (expression.getValueCase() == ATTRIBUTE_EXPRESSION && !isComplexAttribute(expression))
        || (isComplexAttribute(expression))) {
      String col = viewDefinition.getKeyColumnNameForMap(getLogicalColumnName(expression));
      if (col != null && col.length() > 0) {
        return col;
      }
    }
    throw new IllegalArgumentException(
        "operator CONTAINS_KEY/KEYVALUE supports multi value column only");
  }

  private String convertExpressionToMapValueColumn(Expression expression) {
    if ((expression.getValueCase() == COLUMNIDENTIFIER)
        || (expression.getValueCase() == ATTRIBUTE_EXPRESSION && !isComplexAttribute(expression))
        || isComplexAttribute(expression)) {
      String col = viewDefinition.getValueColumnNameForMap(getLogicalColumnName(expression));
      if (col != null && col.length() > 0) {
        return col;
      }
    }
    throw new IllegalArgumentException(
        "operator CONTAINS_KEY/KEYVALUE supports multi value column only");
  }

  private String getLogicalColumnName(Expression expression) {
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

  private LiteralConstant[] convertExpressionToMapLiterals(Expression expression) {
    LiteralConstant[] literals = new LiteralConstant[2];
    if (expression.getValueCase() == LITERAL) {
      LiteralConstant value = expression.getLiteral();
      String[] literalArguments = new String[] {"", ""};

      // backward compatibility
      if (value.getValue().getValueType() == ValueType.STRING_ARRAY) {
        literalArguments[0] = value.getValue().getStringArray(0);
        if (value.getValue().getStringArrayCount() > 1) {
          literalArguments[1] = value.getValue().getStringArray(1);
        }
      } else if (value.getValue().getValueType() == ValueType.STRING) {
        literalArguments[1] = value.getValue().getString();
      } else {
        throw new IllegalArgumentException(
            "operator CONTAINS_KEYVALUE supports "
                + ValueType.STRING_ARRAY.name()
                + " and "
                + ValueType.STRING.name()
                + " value type only");
      }

      for (int i = 0; i < 2; i++) {
        literals[i] =
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setString(literalArguments[i]).build())
                .build();
      }
    }

    for (int i = 0; i < literals.length; i++) {
      if (literals[i] == null) {
        literals[i] =
            LiteralConstant.newBuilder().setValue(Value.newBuilder().setString("").build()).build();
      }
    }

    return literals;
  }

  private boolean isComplexAttribute(Expression expression) {
    return expression.getValueCase().equals(ATTRIBUTE_EXPRESSION)
        && expression.getAttributeExpression().hasSubpath();
  }

  /** TODO:Handle all types */
  private String convertLiteralToString(LiteralConstant literal, Params.Builder paramsBuilder) {
    Value value = literal.getValue();
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
    return ret;
  }
}
