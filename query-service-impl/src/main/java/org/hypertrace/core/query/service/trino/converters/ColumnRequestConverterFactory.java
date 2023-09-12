package org.hypertrace.core.query.service.trino.converters;

import org.hypertrace.core.query.service.trino.TableDefinition;

public class ColumnRequestConverterFactory {

  private ColumnRequestConverterFactory() {
    // empty private constructor
  }

  public static ColumnRequestConverter getColumnRequestConverter(
      TableDefinition tableDefinition, TrinoFunctionConverter functionConverter) {
    return new DefaultColumnRequestConverter(tableDefinition, functionConverter);
  }
}
