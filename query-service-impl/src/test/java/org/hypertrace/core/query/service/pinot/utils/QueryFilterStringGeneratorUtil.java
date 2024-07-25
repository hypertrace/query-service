package org.hypertrace.core.query.service.pinot.utils;

import com.google.protobuf.util.JsonFormat;
import org.hypertrace.core.query.service.QueryRequestUtil;
import org.hypertrace.core.query.service.api.AttributeExpression;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class QueryFilterStringGeneratorUtil {
  private static final JsonFormat.Printer jsonPrinter = JsonFormat.printer();
  private static final String SERVICE_NAME_FILTER_JSON_STRING =
      "{\n"
          + "  \"lhs\": {\n"
          + "    \"attributeExpression\": {\n"
          + "      \"attributeId\": \"SERVICE.name\"\n"
          + "    }\n"
          + "  },\n"
          + "  \"operator\": \"EQ\",\n"
          + "  \"rhs\": {\n"
          + "    \"literal\": {\n"
          + "      \"value\": {\n"
          + "        \"string\": \"dummyService\"\n"
          + "      }\n"
          + "    }\n"
          + "  }\n"
          + "}";

  /*
   Use this method to generate the filter string which can be input to the config path ("additionalTenantFilters.timeRangeAndFilters.filter")

   We have example usage of this method which generates a filter string
    - Filter denotes -> SERVICE.name equals "dummyService".

    Filter can be built in two ways
     1) defining the whole filter.
     2) using the utils method (preferable)
    Both gives same results, either of two can be used based on suitability.
  */
  @Test
  void generateQueryFilterString() throws Exception {
    // 1) defining the whole filter.
    // build lhs of filter
    Expression lhs =
        Expression.newBuilder()
            .setAttributeExpression(AttributeExpression.newBuilder().setAttributeId("SERVICE.name"))
            .build();
    // build rhs of the filter
    Expression rhs =
        Expression.newBuilder()
            .setLiteral(
                LiteralConstant.newBuilder()
                    .setValue(
                        Value.newBuilder()
                            .setValueType(ValueType.STRING)
                            .setString(String.valueOf("dummyService"))))
            .build();

    Filter serviceNameFilter =
        Filter.newBuilder().setLhs(lhs).setRhs(rhs).setOperator(Operator.EQ).build();
    String filterJsonString = jsonPrinter.print(serviceNameFilter);
    Assertions.assertEquals(SERVICE_NAME_FILTER_JSON_STRING, filterJsonString);

    // 2) using the utils method
    serviceNameFilter = QueryRequestUtil.createFilter("SERVICE.name", Operator.EQ, "dummyService");
    filterJsonString = jsonPrinter.print(serviceNameFilter);
    Assertions.assertEquals(SERVICE_NAME_FILTER_JSON_STRING, filterJsonString);
  }
}
