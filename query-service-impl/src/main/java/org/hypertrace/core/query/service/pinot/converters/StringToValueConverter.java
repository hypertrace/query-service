package org.hypertrace.core.query.service.pinot.converters;

import com.google.protobuf.ByteString;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;

/**
 * Converter to convert any string value to requested {@link Value} of {@link ValueType}
 * */
public class StringToValueConverter implements ToValueConverter<String> {

  public static final StringToValueConverter INSTANCE = new StringToValueConverter();
  private static final String EMPTY = "";

  private StringToValueConverter() {
  }

  public Value convert(String value, ValueType valueType) throws Exception {
    Value.Builder valueBuilder = Value.newBuilder();
    switch (valueType) {
      case BYTES:
        String trimmed = StringUtils.trim(value);
        String outValue = (StringUtils.isEmpty(trimmed) || trimmed.equals("null") ||
                trimmed.equals("''")) || trimmed.equals("{}")? EMPTY : value;
        byte[] bytes = Hex.decodeHex(outValue);
        valueBuilder.setBytes(ByteString.copyFrom(bytes));
        valueBuilder.setValueType(ValueType.BYTES);
        break;
      default:
        valueBuilder.setString(value);
        valueBuilder.setValueType(ValueType.STRING);
        break;
    }
    return valueBuilder.build();
  }
}
