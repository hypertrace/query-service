package org.hypertrace.core.query.service.postgres;

import com.typesafe.config.Config;
import java.util.Set;

/**
 * A column level filter that's applied for a Postgres view by default. This is useful to build
 * views which have a subset of data from another view (call it main view) after applying a few
 * filters. The main view and filtered view can be optimized different for queries and only the
 * queries matching the view filters will be routed to the filtered view.
 *
 * <p>Currently, we only support EQ and IN operators in these filters. Example EQ filter: { column:
 * "EVENT.isEntrySpan" operator: "EQ" value: "true" }
 *
 * <p>Example IN filter: { column: "EVENT.statusCode" operator: "IN" values: ["500", "401"] }
 */
class TableColumnFilter {
  private final Operator operator;
  private final Set<String> values;

  enum Operator {
    IN,
    EQ
  }

  public TableColumnFilter(Operator operator, Set<String> values) {
    this.operator = operator;
    this.values = values;
  }

  public static TableColumnFilter from(Config filterConfig) {
    Operator operator = Operator.valueOf(filterConfig.getString("operator"));
    if (operator == Operator.EQ) {
      return new TableColumnFilter(operator, Set.of(filterConfig.getString("value")));
    } else {
      return new TableColumnFilter(operator, Set.copyOf(filterConfig.getStringList("values")));
    }
  }

  public Operator getOperator() {
    return this.operator;
  }

  public Set<String> getValues() {
    return this.values;
  }
}
