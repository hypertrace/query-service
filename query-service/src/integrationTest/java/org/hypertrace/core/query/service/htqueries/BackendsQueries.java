package org.hypertrace.core.query.service.htqueries;

import static org.hypertrace.core.query.service.QueryServiceTestUtils.createFilter;

import java.time.Duration;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.OrderByExpression;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.QueryRequest.Builder;
import org.hypertrace.core.query.service.api.SortOrder;
import org.hypertrace.core.query.service.api.ValueType;

class BackendsQueries {

  /**
   * [ Select backend_id, backend_name, backend_protocol, COUNT(*) FROM backendEntityView WHERE
   * tenant_id = ? AND ( backend_id != ? AND start_time_millis >= ? AND start_time_millis < ? )
   * GROUP BY backend_id, backend_name, backend_protocol ORDER BY PERCENTILETDIGEST99(duration_millis)
   * desc  limit 10000=Params{integerParams={}, longParams={2=1612270796194, 3=1614689996194},
   * stringParams={0=__default, 1=null}, floatParams={}, doubleParams={}, byteStringParams={}} ]
   */
  static QueryRequest buildQuery1() {
    Builder builder = QueryRequest.newBuilder();
    ColumnIdentifier backendId = ColumnIdentifier.newBuilder().setColumnName("BACKEND.id").build();
    ColumnIdentifier backendName = ColumnIdentifier.newBuilder().setColumnName("BACKEND.name").setAlias("BACKEND.name").build();
    ColumnIdentifier backendType = ColumnIdentifier.newBuilder().setColumnName("BACKEND.type").setAlias("BACKEND.type").build();
    Function backendIdCountFunction = Function.newBuilder().addArguments(
        Expression.newBuilder().setColumnIdentifier(backendId).build()).setFunctionName("COUNT")
        .build();

    builder.addSelection(Expression.newBuilder().setColumnIdentifier(backendId).build());
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(backendName).build());
    builder.addSelection(Expression.newBuilder().setFunction(backendIdCountFunction));

    Filter filter1 =
        createFilter(
            "BACKEND.startTime", Operator.GE,
            ValueType.LONG, System.currentTimeMillis() - Duration.ofHours(1).toMillis());
    Filter filter2 =
        createFilter("BACKEND.startTime", Operator.LT,  ValueType.LONG, System.currentTimeMillis());
    Filter filter3 =
        createFilter("BACKEND.id", Operator.NEQ, ValueType.NULL_STRING, "");

    builder.setFilter(
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(filter1)
            .addChildFilter(filter2)
            .addChildFilter(filter3)
            .build());

    builder.addGroupBy(
        Expression.newBuilder().setColumnIdentifier(backendId));
    builder.addGroupBy(
        Expression.newBuilder().setColumnIdentifier(backendName));
    builder.addGroupBy(
        Expression.newBuilder().setColumnIdentifier(backendType));
    Function backendDurationPercentileFunc = Function.newBuilder().addArguments(
        Expression.newBuilder().setColumnIdentifier(
            ColumnIdentifier.newBuilder().setColumnName("BACKEND.duration").build()).build()).setFunctionName("PERCENTILE99")
        .build();
    OrderByExpression orderByExpression = OrderByExpression.newBuilder().setOrder(SortOrder.DESC).setExpression(
        Expression.newBuilder().setFunction(backendDurationPercentileFunc).build()).build();
    builder.addOrderBy(orderByExpression);
    builder.setLimit(10000);

    return builder.build();
  }
}
