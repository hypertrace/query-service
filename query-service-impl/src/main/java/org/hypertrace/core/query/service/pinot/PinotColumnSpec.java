package org.hypertrace.core.query.service.pinot;

import java.util.ArrayList;
import java.util.List;
import org.hypertrace.core.query.service.api.ValueType;

public class PinotColumnSpec {

  private final List<String> columnNames;
  private ValueType type;
  private boolean textIndex;

  public PinotColumnSpec() {
    columnNames = new ArrayList<>();
    textIndex = false;
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

  public boolean hasTextIndex() {
    return textIndex;
  }

  public void setTextIndex() {
    this.textIndex = true;
  }
}
