package org.hypertrace.core.query.service.htqueries;

import static org.hypertrace.core.query.service.QueryServiceTestUtils.createColumnExpression;
import static org.hypertrace.core.query.service.QueryServiceTestUtils.createComplexAttributeExpression;
import static org.hypertrace.core.query.service.QueryServiceTestUtils.createFilter;
import static org.hypertrace.core.query.service.QueryServiceTestUtils.createFunctionExpression;
import static org.hypertrace.core.query.service.QueryServiceTestUtils.createOrderByExpression;
import static org.hypertrace.core.query.service.QueryServiceTestUtils.createStringLiteralValueExpression;
import static org.hypertrace.core.query.service.QueryServiceTestUtils.createTimeColumnGroupByFunctionExpression;

import java.time.Duration;
import java.util.List;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.OrderByExpression;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.QueryRequest.Builder;
import org.hypertrace.core.query.service.api.SortOrder;
import org.hypertrace.core.query.service.api.ValueType;

class ServicesQueries {

  /**
   * [ Select service_id, service_name, COUNT(*) FROM rawServiceView WHERE tenant_id = ? AND (
   * service_id != ? AND start_time_millis >= ? AND start_time_millis < ? ) GROUP BY service_id,
   * service_name ORDER BY PERCENTILETDIGEST99(duration_millis) desc limit
   * 10000=Params{integerParams={}, longParams={2=1612271838043, 3=1614691038043},
   * stringParams={0=__default, 1=null}, floatParams={}, doubleParams={}, byteStringParams={}} ]
   */
  static QueryRequest buildQuery1() {
    Builder builder = QueryRequest.newBuilder();
    Expression serviceId = createColumnExpression("SERVICE.id");
    Expression serviceName = createColumnExpression("SERVICE.name");
    Expression serviceIdFunction = createFunctionExpression("COUNT", serviceId);

    builder.addSelection(serviceId);
    builder.addSelection(serviceName);
    builder.addSelection(serviceIdFunction);

    Filter filter1 =
        createFilter(
            "SERVICE.startTime",
            Operator.GE,
            ValueType.LONG,
            System.currentTimeMillis() - Duration.ofHours(1).toMillis());
    Filter filter2 =
        createFilter("SERVICE.startTime", Operator.LT, ValueType.LONG, System.currentTimeMillis());
    Filter filter3 = createFilter("SERVICE.id", Operator.NEQ, ValueType.NULL_STRING, "");

    builder.setFilter(
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(filter1)
            .addChildFilter(filter2)
            .addChildFilter(filter3)
            .build());

    builder.addGroupBy(serviceId);
    builder.addGroupBy(serviceName);

    Expression serviceDurationPercentileFunc =
        createFunctionExpression("PERCENTILE99", createColumnExpression("SERVICE.duration"));
    OrderByExpression orderByExpression =
        createOrderByExpression(serviceDurationPercentileFunc, SortOrder.DESC);

    builder.addOrderBy(orderByExpression);
    builder.setLimit(10000);
    return builder.build();
  }

  static QueryRequest buildAvgRateQuery() {
    Builder builder = QueryRequest.newBuilder();
    Expression serviceId = createColumnExpression("SERVICE.id");
    Expression serviceDuration = createColumnExpression("SERVICE.duration");
    Expression serviceName = createColumnExpression("SERVICE.name");

    builder.addSelection(serviceId);
    builder.addSelection(serviceName);

    Expression durationSumFunction = createFunctionExpression("SUM", serviceDuration);
    Expression durationAvgRateFunction =
        createFunctionExpression(
            "AVGRATE", serviceDuration, createStringLiteralValueExpression("PT1S"));

    builder.addSelection(durationAvgRateFunction);
    builder.addSelection(durationSumFunction);

    Filter filter1 =
        createFilter(
            "SERVICE.startTime",
            Operator.GE,
            ValueType.LONG,
            System.currentTimeMillis() - Duration.ofHours(1).toMillis());
    Filter filter2 =
        createFilter("SERVICE.startTime", Operator.LT, ValueType.LONG, System.currentTimeMillis());
    Filter filter3 = createFilter("SERVICE.id", Operator.NEQ, ValueType.NULL_STRING, "");

    builder.setFilter(
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(filter1)
            .addChildFilter(filter2)
            .addChildFilter(filter3)
            .build());

    builder.addGroupBy(serviceId);
    builder.addGroupBy(serviceName);

    Expression serviceDurationPercentileFunc =
        createFunctionExpression("PERCENTILE99", serviceDuration);
    OrderByExpression orderByExpression =
        createOrderByExpression(serviceDurationPercentileFunc, SortOrder.DESC);

    builder.addOrderBy(orderByExpression);
    builder.setLimit(10000);
    return builder.build();
  }

  static QueryRequest buildAvgRateQueryForOrderBy() {
    Builder builder = QueryRequest.newBuilder();
    Expression serviceId = createColumnExpression("SERVICE.id");
    Expression serviceName = createColumnExpression("SERVICE.name");

    builder.addSelection(serviceId);
    builder.addSelection(serviceName);

    Expression countFunction = createFunctionExpression("COUNT", serviceId);
    builder.addSelection(countFunction);

    Filter startTimeFilter =
        createFilter(
            "SERVICE.startTime",
            Operator.GE,
            ValueType.LONG,
            System.currentTimeMillis() - Duration.ofHours(1).toMillis());
    Filter endTimeFilter =
        createFilter("SERVICE.startTime", Operator.LT, ValueType.LONG, System.currentTimeMillis());
    Filter nullCheckFilter = createFilter("SERVICE.id", Operator.NEQ, ValueType.NULL_STRING, "");

    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .addChildFilter(nullCheckFilter)
            .build();
    builder.setFilter(andFilter);

    builder.addGroupBy(serviceId);
    builder.addGroupBy(serviceName);

    Expression serviceErrorCount = createColumnExpression("SERVICE.errorCount");
    Expression avgrateFunction = createFunctionExpression("AVGRATE", serviceErrorCount);
    builder.addSelection(avgrateFunction);
    builder.addOrderBy(createOrderByExpression(avgrateFunction, SortOrder.ASC));
    builder.setLimit(10000);
    return builder.build();
  }

  static QueryRequest buildAvgRateQueryWithTimeAggregation() {
    Builder builder = QueryRequest.newBuilder();
    Expression serviceId = createColumnExpression("SERVICE.id");
    builder.addSelection(serviceId);

    Expression serviceNumCalls = createColumnExpression("SERVICE.numCalls");
    Expression durationAvgRateFunction =
        createFunctionExpression(
            "AVGRATE", serviceNumCalls, createStringLiteralValueExpression("PT1S"));
    Expression durationSumFunction = createFunctionExpression("SUM", serviceNumCalls);

    builder.addSelection(durationAvgRateFunction);
    builder.addSelection(durationSumFunction);

    Filter filter1 =
        createFilter(
            "SERVICE.startTime",
            Operator.GE,
            ValueType.LONG,
            System.currentTimeMillis() - Duration.ofHours(1).toMillis());
    Filter filter2 =
        createFilter("SERVICE.startTime", Operator.LT, ValueType.LONG, System.currentTimeMillis());
    Filter filter3 = createFilter("SERVICE.id", Operator.NEQ, ValueType.NULL_STRING, "");

    builder.setFilter(
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(filter1)
            .addChildFilter(filter2)
            .addChildFilter(filter3)
            .build());

    builder.addGroupBy(
        createTimeColumnGroupByFunctionExpression("SERVICE.startTime", "15:SECONDS"));
    builder.addGroupBy(serviceId);
    builder.setLimit(10000);

    return builder.build();
  }

  static QueryRequest buildQueryHavingNullValue() {
    Builder builder = QueryRequest.newBuilder();
    Expression serviceId = createColumnExpression("SERVICE.id");
    builder.addSelection(serviceId);

    Expression protocol = createColumnExpression("TRACE.protocol");
    builder.addSelection(protocol);

    Filter filter1 =
        createFilter(
            "SERVICE.startTime",
            Operator.GE,
            ValueType.LONG,
            System.currentTimeMillis() - Duration.ofHours(1).toMillis());
    Filter filter2 =
        createFilter("SERVICE.startTime", Operator.LT, ValueType.LONG, System.currentTimeMillis());
    Filter filter3 = createFilter("SERVICE.id", Operator.NEQ, ValueType.NULL_STRING, "");

    builder.setFilter(
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(filter1)
            .addChildFilter(filter2)
            .addChildFilter(filter3)
            .build());

    builder.setLimit(10000);

    return builder.build();
  }

  public static QueryRequest buildTraceIdEqualQuery() {
    Builder builder = QueryRequest.newBuilder();
    Expression serviceId = createColumnExpression("SERVICE.id");
    Expression serviceName = createColumnExpression("SERVICE.name");

    builder.addSelection(serviceId);
    builder.addSelection(serviceName);

    Filter filter = createFilter("TRACE.id", Operator.EQ, ValueType.STRING, "8851c75f67562e1f");

    builder.setFilter(filter);

    return builder.build();
  }

  public static QueryRequest buildTraceIdsInQuery() {
    Builder builder = QueryRequest.newBuilder();
    Expression serviceId = createColumnExpression("SERVICE.id");
    Expression serviceName = createColumnExpression("SERVICE.name");

    builder.addSelection(serviceId);
    builder.addSelection(serviceName);

    Filter filter =
        createFilter(
            "TRACE.id",
            Operator.IN,
            ValueType.STRING_ARRAY,
            List.of("8851c75f67562e1f", "d8b0a767ec677cde"));

    builder.setFilter(filter);

    return builder.build();
  }

  public static QueryRequest buildTraceIdNotEmptyQuery() {
    Builder builder = QueryRequest.newBuilder();
    Expression serviceId = createColumnExpression("SERVICE.id");
    Expression serviceName = createColumnExpression("SERVICE.name");

    builder.addSelection(serviceId);
    builder.addSelection(serviceName);

    Filter filter = createFilter("TRACE.id", Operator.NEQ, ValueType.STRING, "");

    builder.setFilter(filter);

    return builder.build();
  }

  public static QueryRequest buildTraceIdIsEmptyQuery() {
    Builder builder = QueryRequest.newBuilder();
    Expression serviceId = createColumnExpression("SERVICE.id");
    Expression serviceName = createColumnExpression("SERVICE.name");

    builder.addSelection(serviceId);
    builder.addSelection(serviceName);

    Filter filter = createFilter("TRACE.id", Operator.EQ, ValueType.STRING, "");

    builder.setFilter(filter);

    return builder.build();
  }

  public static QueryRequest buildTagsContainsKeyQuery() {
    Builder builder = QueryRequest.newBuilder();
    Expression serviceId = createColumnExpression("SERVICE.id");
    Expression serviceName = createColumnExpression("SERVICE.name");

    builder.addSelection(serviceId);
    builder.addSelection(serviceName);

    Filter filter = createFilter("SERVICE.tags", Operator.CONTAINS_KEY, ValueType.STRING, "key4");

    builder.setFilter(filter);

    return builder.build();
  }

  public static QueryRequest buildTagsNotContainsKeyQuery() {
    Builder builder = QueryRequest.newBuilder();
    Expression serviceId = createColumnExpression("SERVICE.id");
    Expression serviceName = createColumnExpression("SERVICE.name");

    builder.addSelection(serviceId);
    builder.addSelection(serviceName);

    Filter filter =
        createFilter("SERVICE.tags", Operator.NOT_CONTAINS_KEY, ValueType.STRING, "key2");

    builder.setFilter(filter);

    return builder.build();
  }

  public static QueryRequest buildTagsContainsKeyValueQuery() {
    Builder builder = QueryRequest.newBuilder();
    Expression serviceId = createColumnExpression("SERVICE.id");
    Expression serviceName = createColumnExpression("SERVICE.name");

    builder.addSelection(serviceId);
    builder.addSelection(serviceName);

    Filter filter =
        createFilter(
            "SERVICE.tags",
            Operator.CONTAINS_KEYVALUE,
            ValueType.STRING_ARRAY,
            List.of("key2", "value23"));

    builder.setFilter(filter);

    return builder.build();
  }

  public static QueryRequest buildTagsContainsKeyLikeQuery() {
    Builder builder = QueryRequest.newBuilder();
    Expression serviceId = createColumnExpression("SERVICE.id");
    Expression serviceName = createColumnExpression("SERVICE.name");

    builder.addSelection(serviceId);
    builder.addSelection(serviceName);

    Filter filter =
        createFilter("SERVICE.tags", Operator.CONTAINS_KEY_LIKE, ValueType.STRING, "KEY5.*");

    builder.setFilter(filter);

    return builder.build();
  }

  public static QueryRequest buildTagsComplexAttrExpEqualQuery() {
    Builder builder = QueryRequest.newBuilder();
    Expression serviceId = createColumnExpression("SERVICE.id");
    Expression serviceName = createColumnExpression("SERVICE.name");

    builder.addSelection(serviceId);
    builder.addSelection(serviceName);

    Expression spanTags = createComplexAttributeExpression("SERVICE.tags", "key2").build();
    Filter filter =
        Filter.newBuilder()
            .setLhs(spanTags)
            .setOperator(Operator.EQ)
            .setRhs(createStringLiteralValueExpression("value23"))
            .build();
    builder.setFilter(filter);

    builder.setFilter(filter);

    return builder.build();
  }

  public static QueryRequest buildLabelsEqualsNullQuery() {
    Builder builder = QueryRequest.newBuilder();
    Expression serviceLabels = createColumnExpression("SERVICE.labels");

    builder.addSelection(serviceLabels);

    Filter filter = createFilter("SERVICE.labels", Operator.EQ, ValueType.NULL_STRING, "");
    builder.setFilter(filter);

    return builder.build();
  }

  public static QueryRequest buildLabelsNotEqualsNullQuery() {
    Builder builder = QueryRequest.newBuilder();
    Expression serviceLabels = createColumnExpression("SERVICE.labels");

    builder.addSelection(serviceLabels);

    Filter filter = createFilter("SERVICE.labels", Operator.NEQ, ValueType.NULL_STRING, "");
    builder.setFilter(filter);

    return builder.build();
  }

  public static QueryRequest buildLabelsEqualsQuery() {
    Builder builder = QueryRequest.newBuilder();
    Expression serviceLabels = createColumnExpression("SERVICE.labels");

    builder.addSelection(serviceLabels);

    Filter filter = createFilter("SERVICE.labels", Operator.EQ, ValueType.STRING, "label1");
    builder.setFilter(filter);

    return builder.build();
  }

  public static QueryRequest buildLabelsInQuery() {
    Builder builder = QueryRequest.newBuilder();
    Expression serviceLabels = createColumnExpression("SERVICE.labels");

    builder.addSelection(serviceLabels);

    Filter filter =
        createFilter(
            "SERVICE.labels", Operator.IN, ValueType.STRING_ARRAY, List.of("label1", "label4"));
    builder.setFilter(filter);

    return builder.build();
  }

  public static QueryRequest buildLabelsNotEqualsQuery() {
    Builder builder = QueryRequest.newBuilder();
    Expression serviceLabels = createColumnExpression("SERVICE.labels");

    builder.addSelection(serviceLabels);

    Filter filter =
        createFilter("SERVICE.labels", Operator.NEQ, ValueType.STRING_ARRAY, List.of("label4"));
    builder.setFilter(filter);

    return builder.build();
  }

  public static QueryRequest buildLabelsNotInQuery() {
    Builder builder = QueryRequest.newBuilder();
    Expression serviceLabels = createColumnExpression("SERVICE.labels");

    builder.addSelection(serviceLabels);

    Filter filter =
        createFilter(
            "SERVICE.labels", Operator.NOT_IN, ValueType.STRING_ARRAY, List.of("label4", "label5"));
    builder.setFilter(filter);

    return builder.build();
  }

  public static QueryRequest buildLabelsDistinctCountQuery() {
    Builder builder = QueryRequest.newBuilder();
    Expression serviceLabels = createColumnExpression("SERVICE.labels");
    Expression distinctCountFunction = createFunctionExpression("DISTINCTCOUNT", serviceLabels);

    builder.addSelection(distinctCountFunction);

    Filter filter = createFilter("SERVICE.labels", Operator.NEQ, ValueType.NULL_STRING, "");
    builder.setFilter(filter);

    return builder.build();
  }
}
