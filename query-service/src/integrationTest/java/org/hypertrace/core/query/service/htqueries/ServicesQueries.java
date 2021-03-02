package org.hypertrace.core.query.service.htqueries;

import static org.hypertrace.core.query.service.Utils.createFilter;

import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.QueryRequest.Builder;
import org.hypertrace.core.query.service.api.ValueType;
import org.junit.jupiter.api.Test;

class ServicesQueries {

  /**
   * Translates to following PQL [ Select service_id, service_name, COUNT(*) FROM rawServiceView
   * WHERE tenant_id = ? AND ( service_id != ? AND start_time_millis >= ? AND start_time_millis < ?
   * ) GROUP BY service_id, service_name ORDER BY PERCENTILETDIGEST99(duration_millis) desc  limit
   * 10000=Params{integerParams={}, longParams={2=1612271838043, 3=1614691038043},
   * stringParams={0=__default, 1=null}, floatParams={}, doubleParams={}, byteStringParams={}} ]
   * <p>
   * childFilter { lhs { columnIdentifier { columnName: "SERVICE.id" } } operator: NEQ rhs { literal
   * { value { valueType: NULL_STRING } } } } childFilter { lhs { columnIdentifier { columnName:
   * "SERVICE.startTime" } } operator: GE rhs { literal { value { valueType: LONG long:
   * 1612271838043 } } } } childFilter { lhs { columnIdentifier { columnName: "SERVICE.startTime" }
   * } operator: LT rhs { literal { value { valueType: LONG long: 1614691038043 } } } } } selection
   * { columnIdentifier { columnName: "SERVICE.id" } } selection { columnIdentifier { columnName:
   * "SERVICE.name" alias: "SERVICE.name" } } selection { function { functionName: "COUNT" arguments
   * { columnIdentifier { columnName: "SERVICE.id" } } } } groupBy { columnIdentifier { columnName:
   * "SERVICE.id" } } groupBy { columnIdentifier { columnName: "SERVICE.name" alias: "SERVICE.name"
   * } } orderBy { expression { function { functionName: "PERCENTILE99" arguments { columnIdentifier
   * { columnName: "SERVICE.duration" } } } } order: DESC } limit: 10000 ]
   */
  //@Test
  void buildQuery1() {
    Builder builder = QueryRequest.newBuilder();
    ColumnIdentifier serviceId = ColumnIdentifier.newBuilder().setColumnName("SERVICE.id").build();
    ColumnIdentifier serviceName = ColumnIdentifier.newBuilder().setColumnName("SERVICE.name")
        .build();
    Function serviceIdFunction = Function.newBuilder().addArguments(
        Expression.newBuilder().setColumnIdentifier(serviceId).build()).setFunctionName("COUNT")
        .build();

    builder.addSelection(Expression.newBuilder().setColumnIdentifier(serviceId).build());
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(serviceName).build());
    builder.addSelection(Expression.newBuilder().setFunction(serviceIdFunction));

    /**
     * childFilter {
     *    *     lhs {
     *    *       columnIdentifier {
     *    *         columnName: "SERVICE.id"
     *    *       }
     *    *     }
     *    *     operator: NEQ
     *    *     rhs {
     *    *       literal {
     *    *         value {
     *    *           valueType: NULL_STRING
     *    *         }
     *    *       }
     *    *     }
     *    *   }
     *    *   childFilter {
     *    *     lhs {
     *    *       columnIdentifier {
     *    *         columnName: "SERVICE.startTime"
     *    *       }
     *    *     }
     *    *     operator: GE
     *    *     rhs {
     *    *       literal {
     *    *         value {
     *    *           valueType: LONG
     *    *           long: 1612271838043
     *    *         }
     *    *       }
     *    *     }
     *    *   }
     *    *   childFilter {
     *    *     lhs {
     *    *       columnIdentifier {
     *    *         columnName: "SERVICE.startTime"
     *    *       }
     *    *     }
     *    *     operator: LT
     *    *     rhs {
     *    *       literal {
     *    *         value {
     *    *           valueType: LONG
     *    *           long: 1614691038043
     *    *         }
     *    *       }
     *    *     }
     *    *   }
     *    * }
     */
    Filter filter1 =
        createFilter(
            "SERVICE.startTime", Operator.GE,
            ValueType.LONG, System.currentTimeMillis() - 1000 * 60 * 60 * 24);
    Filter filter2 =
        createFilter("SERVICE.startTime", Operator.LT,  ValueType.LONG, System.currentTimeMillis());
    Filter filter3 =
        createFilter("SERVICE.id", Operator.NEQ, ValueType.NULL_STRING, "");

    builder.setFilter(
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(filter1)
            .addChildFilter(filter2)
            .addChildFilter(filter3)
            .build());
    QueryRequest qr = builder.build();
    //return builder.build();
  }
}
