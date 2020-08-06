package org.hypertrace.core.query.service.pinot.converters;

import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;

/**
 * An interface to convert an input type to requested {@link Value} based on {@link ValueType}.
 */
public interface ToValueConverter<T> {

  Value convert(T value, ValueType valueType) throws Exception;
}
