package org.hypertrace.core.query.service.postgres.converters;

import org.hypertrace.core.query.service.postgres.TableDefinition;

public class ColumnRequestConverterFactory {

  private ColumnRequestConverterFactory() {
    // empty private constructor
  }

  public static ColumnRequestConverter getColumnRequestConverter(
      TableDefinition tableDefinition, PostgresFunctionConverter functionConverter) {
    return new DefaultColumnRequestConverter(tableDefinition, functionConverter);
  }
}
