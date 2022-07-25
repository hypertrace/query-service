package org.hypertrace.core.query.service;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import org.apache.kafka.common.requests.RequestContext;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.OrderByExpression;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.SortOrder;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;

public class QueryServiceTestUtils {

  private static final String REQUESTS_DIR = "attribute-expression-test-queries";

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
        break;
      case STRING_ARRAY:
        value =
            Value.newBuilder()
                .addAllStringArray((List<String>) valueObject)
                .setValueType(ValueType.STRING_ARRAY)
                .build();
    }
    LiteralConstant constant = LiteralConstant.newBuilder().setValue(value).build();
    Expression rhs = Expression.newBuilder().setLiteral(constant).build();
    return Filter.newBuilder().setLhs(lhs).setOperator(op).setRhs(rhs).build();
  }

  public static Expression createTimeColumnGroupByFunctionExpression(
      String timeColumn, String pinotFormattedDuration) {
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
                                .setValue(Value.newBuilder().setString(pinotFormattedDuration))))
                .build())
        .build();
  }

  public static Expression createColumnExpression(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName))
        .build();
  }

  public static Expression createFunctionExpression(
      String functionName, Expression argumentExpression) {
    return Expression.newBuilder()
        .setFunction(
            Function.newBuilder().setFunctionName(functionName).addArguments(argumentExpression))
        .build();
  }

  public static Expression createFunctionExpression(
      String functionName, Expression argumentExpression1, Expression argumentExpression2) {
    return Expression.newBuilder()
        .setFunction(
            Function.newBuilder()
                .setFunctionName(functionName)
                .addArguments(argumentExpression1)
                .addArguments(argumentExpression2))
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

  public static QueryRequest getAttributeExpressionQuery(QueryRequest originalQueryRequest)
      throws InvalidProtocolBufferException {
    // Serialize into json.
    String json =
        JsonFormat.printer().omittingInsignificantWhitespace().print(originalQueryRequest);
    System.out.println(json);

    // Change for attribute Expression
    json = json.replaceAll("columnIdentifier", "attributeExpression");
    json = json.replaceAll("columnName", "attributeId");
    System.out.println(json);

    // Deserialize and return
    QueryRequest.Builder newBuilder = QueryRequest.newBuilder();
    JsonFormat.parser().merge(json, newBuilder);
    return newBuilder.build();
  }

  public static QueryRequest buildQueryFromJsonFile(String filename) throws IOException {
    String resourceFileName = REQUESTS_DIR + "/" + filename;
    Reader requestJsonStr = readResourceFile(resourceFileName);
    QueryRequest.Builder builder = QueryRequest.newBuilder();
    try {
      JsonFormat.parser().merge(requestJsonStr, builder);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }
    return builder.build();
  }

  private static Reader readResourceFile(String fileName) {
    InputStream in = RequestContext.class.getClassLoader().getResourceAsStream(fileName);
    return new BufferedReader(new InputStreamReader(in));
  }
}
