package org.hypertrace.core.query.service.trino;

import lombok.Value;
import org.hypertrace.core.query.service.api.ValueType;

@Value
public class TrinoColumnSpec {
  String columnName;
  ValueType type;
  // TODO :revisit whether Tdigest is required in Trino, if we decide to do write time aggregation
  //  for metrics.
  boolean isTdigest;
}
