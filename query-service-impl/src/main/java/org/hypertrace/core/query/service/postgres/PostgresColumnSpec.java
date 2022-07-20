package org.hypertrace.core.query.service.postgres;

import lombok.Value;
import org.hypertrace.core.query.service.api.ValueType;

@Value
public class PostgresColumnSpec {
  String columnName;
  ValueType type;
  boolean isTdigest;
}
