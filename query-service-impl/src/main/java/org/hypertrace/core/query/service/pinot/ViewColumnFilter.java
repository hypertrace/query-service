package org.hypertrace.core.query.service.pinot;

import com.typesafe.config.Config;
import java.util.HashSet;
import java.util.Set;

/**
 * A column level filter that's applied for this view by default. Only the queries which have
 * these filters will be routed to this view and the filter isn't propagated to the actual
 * data store.
 *
 * <p>Currently, we only support EQ and IN operators in these filters.</p>
 */
class ViewColumnFilter {
  private final Operator operator;
  private final Set<String> values;

  enum Operator {
    IN, EQ
  }

  public ViewColumnFilter(Operator operator, Set<String> values) {
    this.operator = operator;
    this.values = values;
  }

  public static ViewColumnFilter from(Config filterConfig) {
    Operator operator = Operator.valueOf(filterConfig.getString("operator"));
    return new ViewColumnFilter(operator, new HashSet<>(filterConfig.getStringList("values")));
  }

  public Operator getOperator() {
    return this.operator;
  }

  public Set<String> getValues() {
    return this.values;
  }
}
