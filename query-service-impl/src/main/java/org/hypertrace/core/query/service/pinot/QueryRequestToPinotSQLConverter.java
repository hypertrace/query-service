package org.hypertrace.core.query.service.pinot;

import static org.hypertrace.core.query.service.QueryRequestUtil.getLogicalColumnName;
import static org.hypertrace.core.query.service.QueryRequestUtil.isAttributeExpressionWithSubpath;
import static org.hypertrace.core.query.service.QueryRequestUtil.isSimpleAttributeExpression;
import static org.hypertrace.core.query.service.api.Expression.ValueCase.LITERAL;

import com.google.common.base.Joiner;
import com.google.protobuf.ByteString;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
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
  private static final String TEXT_MATCH_OPERATOR = "TEXT_MATCH";
  private static final String MAP_VALUE = "mapValue";
  private static final int MAP_KEY_INDEX = 0;
  private static final int MAP_VALUE_INDEX = 1;

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
      pqlBuilder.append(convertExpressionToString(expr, paramsBuilder, executionContext));
      delim = ", ";
    }

    pqlBuilder.append(" FROM ").append(viewDefinition.getViewName());

    // Add the tenantId filter
    pqlBuilder.append(" WHERE ").append(viewDefinition.getTenantIdColumn()).append(" = ?");
    paramsBuilder.addStringParam(executionContext.getTenantId());

    if (request.hasFilter()) {
      pqlBuilder.append(" AND ");
      String filterClause =
          convertFilterToString(request.getFilter(), paramsBuilder, executionContext);
      pqlBuilder.append(filterClause);
    }

    if (request.getGroupByCount() > 0) {
      pqlBuilder.append(" GROUP BY ");
      delim = "";
      for (Expression groupByExpression : request.getGroupByList()) {
        pqlBuilder.append(delim);
        pqlBuilder.append(
            convertExpressionToString(groupByExpression, paramsBuilder, executionContext));
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
                orderByExpression.getExpression(), paramsBuilder, executionContext));
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

  private String convertFilterToString(
      Filter filter, Builder paramsBuilder, ExecutionContext executionContext) {
    StringBuilder builder = new StringBuilder();
    String operator = convertOperatorToString(filter.getOperator());
    if (filter.getChildFilterCount() > 0) {
      String delim = "";
      builder.append("( ");
      for (Filter childFilter : filter.getChildFilterList()) {
        builder.append(delim);
        builder.append(convertFilterToString(childFilter, paramsBuilder, executionContext));
        builder.append(" ");
        delim = operator + " ";
      }
      builder.append(")");
    } else {
      switch (filter.getOperator()) {
        case LIKE:
          /**
           * If the text index is not enabled on lhs expression, - the pql looks like
           * `regexp_like(lhs, rhs)` else - the pql looks like `text_match(lhs, rhs)`
           */
          operator = handleLikeOperatorConversion(filter.getLhs());
          Expression rhs =
              handleValueConversionForLikeArgument(operator, filter.getLhs(), filter.getRhs());

          builder.append(operator);
          builder.append("(");
          builder.append(
              convertExpressionToString(filter.getLhs(), paramsBuilder, executionContext));
          builder.append(",");
          builder.append(convertExpressionToString(rhs, paramsBuilder, executionContext));
          builder.append(")");
          break;
        case CONTAINS_KEY:
        case NOT_CONTAINS_KEY:
          List<LiteralConstant> kvp = convertExpressionToMapLiterals(filter.getRhs());
          builder.append(convertExpressionToMapKeyColumn(filter.getLhs()));
          builder.append(" ");
          builder.append(operator);
          builder.append(" ");
          builder.append(convertLiteralToString(kvp.get(MAP_KEY_INDEX), paramsBuilder));
          break;
        case CONTAINS_KEYVALUE:
          kvp = convertExpressionToMapLiterals(filter.getRhs());
          String keyCol = convertExpressionToMapKeyColumn(filter.getLhs());
          String valCol = convertExpressionToMapValueColumn(filter.getLhs());
          builder.append(keyCol);
          builder.append(" = ");
          builder.append(convertLiteralToString(kvp.get(MAP_KEY_INDEX), paramsBuilder));
          builder.append(" AND ");
          builder.append(valCol);
          builder.append(" = ");
          builder.append(convertLiteralToString(kvp.get(MAP_VALUE_INDEX), paramsBuilder));
          builder.append(" AND ");
          builder.append(MAP_VALUE);
          builder.append("(");
          builder.append(keyCol);
          builder.append(",");
          builder.append(convertLiteralToString(kvp.get(MAP_KEY_INDEX), paramsBuilder));
          builder.append(",");
          builder.append(valCol);
          builder.append(") = ");
          builder.append(convertLiteralToString(kvp.get(MAP_VALUE_INDEX), paramsBuilder));
          break;
        case CONTAINS_KEY_LIKE:
          kvp = convertExpressionToMapLiterals(filter.getRhs());
          builder.append(operator);
          builder.append("(");
          builder.append(convertExpressionToMapKeyColumn(filter.getLhs()));
          builder.append(",");
          builder.append(convertLiteralToString(kvp.get(MAP_KEY_INDEX), paramsBuilder));
          builder.append(")");
          break;
        default:
          rhs = handleValueConversionForLiteralExpression(filter.getLhs(), filter.getRhs());
          applyValuesFilterForMapColumn(filter, paramsBuilder, executionContext, builder, rhs);
          builder.append(
              convertExpressionToString(filter.getLhs(), paramsBuilder, executionContext));
          builder.append(" ");
          builder.append(operator);
          builder.append(" ");
          builder.append(convertExpressionToString(rhs, paramsBuilder, executionContext));
      }
    }
    return builder.toString();
  }

  /**
   * This helper method adds an additional "__values" filter when dealing with complex attribute
   * expressions with an equal filter. This approach is similar to the deprecated
   * "CONTAINS_KEYVALUE" filter.
   *
   * <p>We always include the "contains_key" filter {@link
   * AttributeExpressionSubpathExistsFilteringTransformation}. In the previous implementation of
   * CONTAINS_KEYVALUE, we also added the "__values" filter. By including this additional filter, if
   * there is an inverted index on a map field, it can be utilized to improve performance.
   */
  private void applyValuesFilterForMapColumn(
      Filter filter,
      Builder paramsBuilder,
      ExecutionContext executionContext,
      StringBuilder builder,
      Expression rhs) {
    if (isAttributeExpressionWithSubpath(filter.getLhs())
        && filter.getOperator().equals(Operator.EQ)) {
      String valueFilter =
          getValuesFilterForMapColumn(filter.getLhs(), rhs, paramsBuilder, executionContext);
      builder.append(valueFilter);
      builder.append(" AND ");
    }
  }

  /**
   * This helper method constructs a "__values" filter for a map column. For example, if the map
   * column is called "tags", it returns "tags__VALUES = 'value'".
   */
  private String getValuesFilterForMapColumn(
      Expression lhs, Expression rhs, Builder paramsBuilder, ExecutionContext executionContext) {
    String valCol = convertExpressionToMapValueColumn(lhs);
    return String.format(
        "%s %s %s",
        valCol,
        convertOperatorToString(Operator.EQ),
        convertExpressionToString(rhs, paramsBuilder, executionContext));
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

  private String handleLikeOperatorConversion(Expression expression) {
    Optional<String> logicalColumnName = getLogicalColumnName(expression);
    if (logicalColumnName.isPresent()
        && isSimpleAttributeExpression(expression)
        && viewDefinition.hasTextIndex(logicalColumnName.get())) {
      return TEXT_MATCH_OPERATOR;
    }
    return REGEX_OPERATOR;
  }

  private Expression handleValueConversionForLikeArgument(
      String likeOperatorStr, Expression lhsExpression, Expression rhsExpression) {
    return postProcessValueConversionForLikeOperator(
        likeOperatorStr, handleValueConversionForLiteralExpression(lhsExpression, rhsExpression));
  }

  private Expression postProcessValueConversionForLikeOperator(
      String likeOperatorStr, Expression rhsExpression) {
    Expression.Builder builder = rhsExpression.toBuilder();
    if (likeOperatorStr.equals(TEXT_MATCH_OPERATOR)) {
      String strValue = "/" + rhsExpression.getLiteral().getValue().getString() + "/";
      builder.getLiteralBuilder().getValueBuilder().setString(strValue);
    }
    return builder.build();
  }

  private String convertExpressionToString(
      Expression expression, Builder paramsBuilder, ExecutionContext executionContext) {
    switch (expression.getValueCase()) {
      case COLUMNIDENTIFIER:
        // this takes care of the Map Type where it's split into 2 columns
        List<String> columnNames =
            viewDefinition.getPhysicalColumnNames(
                getLogicalColumnName(expression).orElseThrow(IllegalArgumentException::new));
        return joiner.join(columnNames);
      case ATTRIBUTE_EXPRESSION:
        if (isAttributeExpressionWithSubpath(expression)) {
          String keyCol = convertExpressionToMapKeyColumn(expression);
          String valCol = convertExpressionToMapValueColumn(expression);
          String pathExpression = expression.getAttributeExpression().getSubpath();
          LiteralConstant pathExpressionLiteral =
              LiteralConstant.newBuilder()
                  .setValue(Value.newBuilder().setString(pathExpression).build())
                  .build();

          return String.format(
              "%s(%s,%s,%s)",
              MAP_VALUE,
              keyCol,
              convertLiteralToString(pathExpressionLiteral, paramsBuilder),
              valCol);
        } else {
          // this takes care of the Map Type where it's split into 2 columns
          columnNames =
              viewDefinition.getPhysicalColumnNames(
                  getLogicalColumnName(expression).orElseThrow(IllegalArgumentException::new));
          return joiner.join(columnNames);
        }
      case LITERAL:
        return convertLiteralToString(expression.getLiteral(), paramsBuilder);
      case FUNCTION:
        return this.functionConverter.convert(
            executionContext,
            expression.getFunction(),
            argExpression ->
                convertExpressionToString(argExpression, paramsBuilder, executionContext));
      case ORDERBY:
        OrderByExpression orderBy = expression.getOrderBy();
        return convertExpressionToString(orderBy.getExpression(), paramsBuilder, executionContext);
      case VALUE_NOT_SET:
        break;
    }
    return "";
  }

  private String convertExpressionToMapKeyColumn(Expression expression) {
    String col =
        viewDefinition.getKeyColumnNameForMap(
            getLogicalColumnName(expression).orElseThrow(IllegalArgumentException::new));
    if (col != null && col.length() > 0) {
      return col;
    }
    throw new IllegalArgumentException("operator supports multi value column only");
  }

  private String convertExpressionToMapValueColumn(Expression expression) {
    String col =
        viewDefinition.getValueColumnNameForMap(
            getLogicalColumnName(expression).orElseThrow(IllegalArgumentException::new));
    if (col != null && col.length() > 0) {
      return col;
    }
    throw new IllegalArgumentException("operator supports multi value column only");
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
      case BOOLEAN_ARRAY:
        builder = new StringBuilder("(");
        delim = "";
        for (Boolean item : value.getBooleanArrayList()) {
          builder.append(delim);
          builder.append(QUESTION_MARK);
          paramsBuilder.addBooleanParam(item);
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
      case UNRECOGNIZED:
        break;
    }
    return ret;
  }
}
