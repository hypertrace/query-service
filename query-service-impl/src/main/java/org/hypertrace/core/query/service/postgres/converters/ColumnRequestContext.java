package org.hypertrace.core.query.service.postgres.converters;

import org.hypertrace.core.query.service.api.ValueType;

class ColumnRequestContext {
  private QueryPart queryPart;
  private ValueType columnValueType;

  private ColumnRequestContext(QueryPart queryPart) {
    this.queryPart = queryPart;
  }

  static ColumnRequestContext createColumnRequestContext(QueryPart queryPart) {
    return new ColumnRequestContext(queryPart);
  }

  void setColumnValueType(ValueType columnValueType) {
    this.columnValueType = columnValueType;
  }

  boolean isSelect() {
    return queryPart.equals(QueryPart.SELECT);
  }

  boolean isArrayColumnType() {
    return columnValueType != null && columnValueType.equals(ValueType.STRING_ARRAY);
  }

  boolean isBytesColumnType() {
    return columnValueType != null && columnValueType.equals(ValueType.BYTES);
  }

  boolean isMapColumnType() {
    return columnValueType != null && columnValueType.equals(ValueType.STRING_MAP);
  }

  enum QueryPart {
    SELECT,
    FILTER,
    ORDER_BY,
    GROUP_BY
  }
}
