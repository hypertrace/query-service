package org.hypertrace.core.query.service.function;

import static org.hypertrace.core.query.service.function.DistinctCountFunctionTransformation.DISTINCT_COUNT;
import static org.hypertrace.core.query.service.function.DistinctCountFunctionTransformation.DISTINCT_COUNT_MV;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.reactivex.rxjava3.core.Single;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.query.service.api.AttributeExpression;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DistinctCountTransformationTest {
  private static final String ID = "id";
  private CachingAttributeClient attributeClient;
  private DistinctCountFunctionTransformation transformation;

  @BeforeEach
  void setUp() {
    this.attributeClient = mock(CachingAttributeClient.class);
    this.transformation = new DistinctCountFunctionTransformation(attributeClient);
  }

  @Test
  void testDistinctCountTransformation() {
    // function without distinct count
    when(attributeClient.get(any())).thenReturn(Single.just(getMockArrayAttributeMetadata()));
    Function function =
        Function.newBuilder()
            .setFunctionName("COUNT")
            .addArguments(
                Expression.newBuilder()
                    .setAttributeExpression(AttributeExpression.newBuilder().setAttributeId(ID)))
            .build();
    assertEquals(
        Expression.newBuilder().setFunction(function).build(),
        this.transformation.transformFunction(function).blockingGet());

    // function with distinct count on non-array
    when(attributeClient.get(any())).thenReturn(Single.just(getMockNonArrayAttributeMetadata()));
    function = function.toBuilder().setFunctionName(DISTINCT_COUNT).build();
    assertEquals(
        Expression.newBuilder().setFunction(function).build(),
        this.transformation.transformFunction(function).blockingGet());

    // function with distinct count on array
    when(attributeClient.get(any())).thenReturn(Single.just(getMockArrayAttributeMetadata()));
    function = function.toBuilder().setFunctionName(DISTINCT_COUNT).build();
    assertEquals(
        Expression.newBuilder()
            .setFunction(function.toBuilder().setFunctionName(DISTINCT_COUNT_MV))
            .build(),
        this.transformation.transformFunction(function).blockingGet());
  }

  private AttributeMetadata getMockNonArrayAttributeMetadata() {
    return AttributeMetadata.newBuilder().setId(ID).setValueKind(AttributeKind.TYPE_STRING).build();
  }

  private AttributeMetadata getMockArrayAttributeMetadata() {
    return AttributeMetadata.newBuilder()
        .setId(ID)
        .setValueKind(AttributeKind.TYPE_STRING_ARRAY)
        .build();
  }
}
