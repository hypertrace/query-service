package org.hypertrace.core.query.service.htqueries;

import static org.hypertrace.core.query.service.Utils.createFilter;

import java.time.Duration;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.QueryRequest.Builder;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;

class ExplorerQueries {

  /**
   * [ Select dateTimeConvert(start_time_millis,?,?,?), COUNT(*) FROM spanEventView WHERE tenant_id = ?
   * AND ( ( start_time_millis >= ? AND start_time_millis < ? ) AND ( api_boundary_type = ? AND api_id != ? ) )
   * GROUP BY dateTimeConvert(start_time_millis,?,?,?) limit 5000000=Params{integerParams={},
   * longParams={4=1612288800000, 5=1614686400000}, stringParams={0=1:MILLISECONDS:EPOCH, 1=1:MILLISECONDS:EPOCH,
   * 2=21600:SECONDS, 3=__default, 6=ENTRY, 7=null, 8=1:MILLISECONDS:EPOCH, 9=1:MILLISECONDS:EPOCH, 10=21600:SECONDS},
   * floatParams={}, doubleParams={}, byteStringParams={}} ]
   */
  static QueryRequest buildQuery1() {
    Builder builder = QueryRequest.newBuilder();

    ColumnIdentifier apiTraceCalls = ColumnIdentifier.newBuilder().setColumnName("API_TRACE.calls").build();
    Function apiTraceCallsFunctionCount = Function.newBuilder().addArguments(
        Expression.newBuilder().setColumnIdentifier(apiTraceCalls).build())
        .setFunctionName("COUNT").setAlias("COUNT_API_TRACE.calls_[]")
        .build();
    builder.addSelection(Expression.newBuilder().setFunction(apiTraceCallsFunctionCount));

    Filter filter1 =
        createFilter(
            "API_TRACE.startTime", Operator.GE,
            ValueType.LONG, System.currentTimeMillis() - Duration.ofHours(1).toMillis());
    Filter filter2 =
        createFilter("API_TRACE.startTime", Operator.LT,  ValueType.LONG, System.currentTimeMillis());
    Filter filter3 =
        createFilter("API_TRACE.apiBoundaryType", Operator.EQ, ValueType.STRING, "ENTRY");
    Filter filter4 =
        createFilter("API_TRACE.apiId", Operator.NEQ, ValueType.STRING, "null");

    builder.setFilter(
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(filter1)
            .addChildFilter(filter2)
            .addChildFilter(filter3)
            .addChildFilter(filter4)
            .build());

    Function dateTimeConvert = Function.newBuilder().setFunctionName("dateTimeConvert")
        .addArguments(Expression.newBuilder().setColumnIdentifier(
            ColumnIdentifier.newBuilder().setColumnName("API_TRACE.startTime").build()).build())
        .addArguments(Expression.newBuilder().setLiteral(LiteralConstant.newBuilder().setValue(
            Value.newBuilder().setString("1:MILLISECONDS:EPOCH")).build()).build())
        .addArguments(Expression.newBuilder().setLiteral(LiteralConstant.newBuilder().setValue(
            Value.newBuilder().setString("1:MILLISECONDS:EPOCH")).build()).build())
        .addArguments(Expression.newBuilder().setLiteral(LiteralConstant.newBuilder().setValue(
            Value.newBuilder().setString("21600:SECONDS")).build()).build())
        .build();
    builder.addGroupBy(Expression.newBuilder().setFunction(dateTimeConvert).build());
    return builder.build();
  }
}
