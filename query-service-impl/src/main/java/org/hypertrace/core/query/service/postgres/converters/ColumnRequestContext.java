package org.hypertrace.core.query.service.postgres.converters;

import org.hypertrace.core.query.service.api.ValueType;

public class ColumnRequestContext {
  private QueryPart queryPart;
  private ValueType columnValueType;

  public void setQueryPart(QueryPart queryPart) {
    this.queryPart = queryPart;
  }

  public void setColumnValueType(ValueType columnValueType) {
    this.columnValueType = columnValueType;
  }

  boolean isSelect() {
    return queryPart.equals(QueryPart.SELECT);
  }

  boolean isBytesColumnType() {
    return columnValueType != null && columnValueType.equals(ValueType.BYTES);
  }

  boolean isMapColumnType() {
    return columnValueType != null && columnValueType.equals(ValueType.STRING_MAP);
  }

  public enum QueryPart {
    SELECT,
    WHERE,
    ORDER_BY,
    GROUP_BY
  }
}
