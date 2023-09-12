package org.hypertrace.core.query.service.trino;

import lombok.Value;
import org.hypertrace.core.query.service.api.ValueType;

@Value
public class TrinoColumnSpec {
  String columnName;
  ValueType type;
  boolean isTdigest;
}
