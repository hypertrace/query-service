package org.hypertrace.core.query.service.postgres;

import java.util.ArrayList;
import java.util.List;
import org.hypertrace.core.query.service.api.ValueType;

public class PostgresColumnSpec {

  private final List<String> columnNames;
  private ValueType type;

  public PostgresColumnSpec() {
    columnNames = new ArrayList<>();
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public void addColumnName(String columnName) {
    columnNames.add(columnName);
  }

  public ValueType getType() {
    return type;
  }

  public void setType(ValueType type) {
    this.type = type;
  }
}
