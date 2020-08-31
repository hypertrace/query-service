package org.hypertrace.core.query.service.pinot;

import com.google.protobuf.ByteString;
import org.apache.commons.codec.DecoderException;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;
import org.hypertrace.core.query.service.pinot.converters.StringToValueConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StringToValueConverterTest {

  @Test
  public void testValidNullFieldsForBytesColumn() throws Exception {
    StringToValueConverter instance = StringToValueConverter.INSTANCE;

    Value value = instance.convert("", ValueType.BYTES);
    Assertions.assertNotNull(value);
    Assertions.assertEquals(ValueType.BYTES, value.getValueType());
    Assertions.assertEquals(ByteString.EMPTY, value.getBytes());

    value = instance.convert("null", ValueType.BYTES);
    Assertions.assertNotNull(value);
    Assertions.assertEquals(ValueType.BYTES, value.getValueType());
    Assertions.assertEquals(ByteString.EMPTY, value.getBytes());

    value = instance.convert("''", ValueType.BYTES);
    Assertions.assertNotNull(value);
    Assertions.assertEquals(ValueType.BYTES, value.getValueType());
    Assertions.assertEquals(ByteString.EMPTY, value.getBytes());

    value = instance.convert("{}", ValueType.BYTES);
    Assertions.assertNotNull(value);
    Assertions.assertEquals(ValueType.BYTES, value.getValueType());
    Assertions.assertEquals(ByteString.EMPTY, value.getBytes());
  }

  @Test
  public void testInValidNullFieldsForBytesColumn() {
    StringToValueConverter instance = StringToValueConverter.INSTANCE;
    Assertions.assertThrows(DecoderException.class, () -> {
      Value value = instance.convert("abc", ValueType.BYTES);
    });
  }
}
