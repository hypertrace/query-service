package org.hypertrace.core.query.service.projection;

import static java.util.Arrays.asList;
import static org.hypertrace.core.attribute.service.v1.ProjectionOperator.PROJECTION_OPERATOR_CONCAT;
import static org.hypertrace.core.attribute.service.v1.ProjectionOperator.PROJECTION_OPERATOR_CONDITIONAL;
import static org.hypertrace.core.attribute.service.v1.ProjectionOperator.PROJECTION_OPERATOR_HASH;
import static org.hypertrace.core.attribute.service.v1.ProjectionOperator.PROJECTION_OPERATOR_STRING_EQUALS;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_AVG;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_CONCAT;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_CONDITIONAL;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_HASH;
import static org.hypertrace.core.query.service.QueryFunctionConstants.QUERY_FUNCTION_STRINGEQUALS;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createAliasedAttributeExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createAliasedComplexAttributeExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createAliasedFunctionExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createCompositeFilter;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createFilter;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createFunctionExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createOrderByExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createStringArrayLiteralValueExpression;
import static org.hypertrace.core.query.service.QueryRequestUtil.createNullNumberLiteralExpression;
import static org.hypertrace.core.query.service.QueryRequestUtil.createNullStringLiteralExpression;
import static org.hypertrace.core.query.service.QueryRequestUtil.createSimpleAttributeExpression;
import static org.hypertrace.core.query.service.QueryRequestUtil.createStringLiteralValueExpression;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import io.reactivex.rxjava3.core.Single;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.projection.AttributeProjectionRegistry;
import org.hypertrace.core.attribute.service.v1.AttributeDefinition;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.attribute.service.v1.Projection;
import org.hypertrace.core.attribute.service.v1.ProjectionExpression;
import org.hypertrace.core.attribute.service.v1.ProjectionOperator;
import org.hypertrace.core.query.service.QueryTransformation.QueryTransformationContext;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.SortOrder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProjectionTransformationTest {

  private static final String PROJECTED_ATTRIBUTE_ID = "PROJECTED_ATTRIBUTE_ID";
  private static final String SIMPLE_ATTRIBUTE_ID = "SIMPLE_ATTRIBUTE_ID";

  @Mock CachingAttributeClient mockAttributeClient;
  @Mock QueryTransformationContext mockTransformationContext;
  private AttributeMetadata attributeMetadata;

  ProjectionTransformation projectionTransformation;

  @BeforeEach
  void beforeEach() {
    this.attributeMetadata =
        AttributeMetadata.newBuilder()
            .setDefinition(
                AttributeDefinition.newBuilder()
                    .setProjection(Projection.newBuilder().setAttributeId(SIMPLE_ATTRIBUTE_ID)))
            .build();

    this.projectionTransformation =
        new ProjectionTransformation(this.mockAttributeClient, new AttributeProjectionRegistry());
  }

  @Test
  void transformsBasicAliasProjection() {
    this.mockAttribute(PROJECTED_ATTRIBUTE_ID, this.attributeMetadata);
    this.mockAttribute(SIMPLE_ATTRIBUTE_ID, AttributeMetadata.getDefaultInstance());
    QueryRequest originalRequest =
        QueryRequest.newBuilder()
            .addSelection(createSimpleAttributeExpression(PROJECTED_ATTRIBUTE_ID))
            .build();
    QueryRequest expectedTransform =
        QueryRequest.newBuilder()
            .addSelection(
                createAliasedAttributeExpression(SIMPLE_ATTRIBUTE_ID, PROJECTED_ATTRIBUTE_ID))
            .build();

    assertEquals(
        expectedTransform,
        this.projectionTransformation
            .transform(originalRequest, mockTransformationContext)
            .blockingGet());
  }

  @Test
  void transformsBasicAliasProjectionWithSubpath() {
    this.mockAttribute(PROJECTED_ATTRIBUTE_ID, this.attributeMetadata);
    this.mockAttribute(SIMPLE_ATTRIBUTE_ID, AttributeMetadata.getDefaultInstance());
    QueryRequest originalRequest =
        QueryRequest.newBuilder()
            .addSelection(
                createAliasedComplexAttributeExpression(
                    PROJECTED_ATTRIBUTE_ID, "test-sub-path", "my-alias"))
            .build();
    QueryRequest expectedTransform =
        QueryRequest.newBuilder()
            .addSelection(
                createAliasedComplexAttributeExpression(
                    SIMPLE_ATTRIBUTE_ID, "test-sub-path", "my-alias"))
            .build();

    assertEquals(
        expectedTransform,
        this.projectionTransformation
            .transform(originalRequest, mockTransformationContext)
            .blockingGet());
  }

  @Test
  void transformsComplexFunctionProjection() {
    // CONCAT(HASH(SIMPLE_ATTRIBUTE_ID), "projectionLiteral")
    Projection projection =
        functionProjection(
            projectionExpression(
                PROJECTION_OPERATOR_CONCAT,
                functionProjection(
                    projectionExpression(
                        PROJECTION_OPERATOR_HASH, attributeIdProjection(SIMPLE_ATTRIBUTE_ID))),
                literalProjection("projectionLiteral")));
    this.attributeMetadata =
        this.attributeMetadata.toBuilder()
            .setDefinition(AttributeDefinition.newBuilder().setProjection(projection))
            .build();
    this.mockAttribute(PROJECTED_ATTRIBUTE_ID, this.attributeMetadata);
    this.mockAttribute(SIMPLE_ATTRIBUTE_ID, AttributeMetadata.getDefaultInstance());

    QueryRequest originalRequest =
        QueryRequest.newBuilder()
            .addSelection(createSimpleAttributeExpression(PROJECTED_ATTRIBUTE_ID))
            .build();

    QueryRequest expectedTransform =
        QueryRequest.newBuilder()
            .addSelection(
                createAliasedFunctionExpression(
                    QUERY_FUNCTION_CONCAT,
                    PROJECTED_ATTRIBUTE_ID,
                    createFunctionExpression(
                        QUERY_FUNCTION_HASH,
                        createSimpleAttributeExpression(SIMPLE_ATTRIBUTE_ID).build()),
                    createStringLiteralValueExpression("projectionLiteral")))
            .build();

    assertEquals(
        expectedTransform,
        this.projectionTransformation
            .transform(originalRequest, mockTransformationContext)
            .blockingGet());
  }

  @Test
  void transformsConditionalAndStringEquals() {
    // CONDITIONAL(STRINGEQUALS(SIMPLE_ATTRIBUTE_ID, "foo"), HASH(SIMPLE_ATTRIBUTE_ID),
    // "projectionLiteral")
    Projection projection =
        functionProjection(
            projectionExpression(
                PROJECTION_OPERATOR_CONDITIONAL,
                functionProjection(
                    projectionExpression(
                        PROJECTION_OPERATOR_STRING_EQUALS,
                        attributeIdProjection(SIMPLE_ATTRIBUTE_ID),
                        literalProjection("foo"))),
                functionProjection(
                    projectionExpression(
                        PROJECTION_OPERATOR_HASH, attributeIdProjection(SIMPLE_ATTRIBUTE_ID))),
                literalProjection("projectionLiteral")));

    this.attributeMetadata =
        this.attributeMetadata.toBuilder()
            .setDefinition(AttributeDefinition.newBuilder().setProjection(projection))
            .build();
    this.mockAttribute(PROJECTED_ATTRIBUTE_ID, this.attributeMetadata);
    this.mockAttribute(SIMPLE_ATTRIBUTE_ID, AttributeMetadata.getDefaultInstance());

    QueryRequest originalRequest =
        QueryRequest.newBuilder()
            .addSelection(createSimpleAttributeExpression(PROJECTED_ATTRIBUTE_ID))
            .build();

    QueryRequest expectedTransform =
        QueryRequest.newBuilder()
            .addSelection(
                createAliasedFunctionExpression(
                    QUERY_FUNCTION_CONDITIONAL,
                    PROJECTED_ATTRIBUTE_ID,
                    createFunctionExpression(
                        QUERY_FUNCTION_STRINGEQUALS,
                        createSimpleAttributeExpression(SIMPLE_ATTRIBUTE_ID).build(),
                        createStringLiteralValueExpression("foo")),
                    createFunctionExpression(
                        QUERY_FUNCTION_HASH,
                        createSimpleAttributeExpression(SIMPLE_ATTRIBUTE_ID).build()),
                    createStringLiteralValueExpression("projectionLiteral")))
            .build();

    assertEquals(
        expectedTransform,
        this.projectionTransformation
            .transform(originalRequest, mockTransformationContext)
            .blockingGet());
  }

  @Test
  void transformsAggregations() {
    this.mockAttribute(PROJECTED_ATTRIBUTE_ID, this.attributeMetadata);
    this.mockAttribute(SIMPLE_ATTRIBUTE_ID, AttributeMetadata.getDefaultInstance());
    QueryRequest originalRequest =
        QueryRequest.newBuilder()
            .addAggregation(
                createAliasedFunctionExpression(
                    QUERY_FUNCTION_AVG,
                    "myAlias",
                    createSimpleAttributeExpression(PROJECTED_ATTRIBUTE_ID).build()))
            .build();
    QueryRequest expectedTransform =
        QueryRequest.newBuilder()
            .addAggregation(
                createAliasedFunctionExpression(
                    QUERY_FUNCTION_AVG,
                    "myAlias",
                    createAliasedAttributeExpression(SIMPLE_ATTRIBUTE_ID, PROJECTED_ATTRIBUTE_ID)))
            .build();

    assertEquals(
        expectedTransform,
        this.projectionTransformation
            .transform(originalRequest, mockTransformationContext)
            .blockingGet());
  }

  @Test
  void transformsOrderBys() {
    this.mockAttribute(PROJECTED_ATTRIBUTE_ID, this.attributeMetadata);
    this.mockAttribute(SIMPLE_ATTRIBUTE_ID, AttributeMetadata.getDefaultInstance());
    QueryRequest originalRequest =
        QueryRequest.newBuilder()
            .addSelection(createSimpleAttributeExpression(PROJECTED_ATTRIBUTE_ID))
            .addOrderBy(
                createOrderByExpression(
                    createSimpleAttributeExpression(PROJECTED_ATTRIBUTE_ID), SortOrder.DESC))
            .build();
    QueryRequest expectedTransform =
        QueryRequest.newBuilder()
            .addSelection(
                createAliasedAttributeExpression(SIMPLE_ATTRIBUTE_ID, PROJECTED_ATTRIBUTE_ID))
            .addOrderBy(
                createOrderByExpression(
                    createAliasedAttributeExpression(SIMPLE_ATTRIBUTE_ID, PROJECTED_ATTRIBUTE_ID)
                        .toBuilder(),
                    SortOrder.DESC))
            .build();

    assertEquals(
        expectedTransform,
        this.projectionTransformation
            .transform(originalRequest, mockTransformationContext)
            .blockingGet());
  }

  @Test
  void transformsNestedFilters() {
    this.mockAttribute(PROJECTED_ATTRIBUTE_ID, this.attributeMetadata);
    this.mockAttribute(SIMPLE_ATTRIBUTE_ID, AttributeMetadata.getDefaultInstance());
    QueryRequest originalRequest =
        QueryRequest.newBuilder()
            .addSelection(createSimpleAttributeExpression(PROJECTED_ATTRIBUTE_ID))
            .setFilter(
                createCompositeFilter(
                    Operator.OR,
                    createFilter(
                        createSimpleAttributeExpression(PROJECTED_ATTRIBUTE_ID).build(),
                        Operator.IN,
                        createStringArrayLiteralValueExpression(List.of("foo", "bar"))),
                    createFilter(
                        createSimpleAttributeExpression(SIMPLE_ATTRIBUTE_ID).build(),
                        Operator.EQ,
                        createStringLiteralValueExpression("otherValue"))))
            .build();

    QueryRequest expectedTransform =
        QueryRequest.newBuilder()
            .addSelection(
                createAliasedAttributeExpression(SIMPLE_ATTRIBUTE_ID, PROJECTED_ATTRIBUTE_ID))
            .setFilter(
                createCompositeFilter(
                    Operator.OR,
                    createFilter(
                        createAliasedAttributeExpression(
                            SIMPLE_ATTRIBUTE_ID, PROJECTED_ATTRIBUTE_ID),
                        Operator.IN,
                        createStringArrayLiteralValueExpression(List.of("foo", "bar"))),
                    createFilter(
                        createSimpleAttributeExpression(SIMPLE_ATTRIBUTE_ID).build(),
                        Operator.EQ,
                        createStringLiteralValueExpression("otherValue"))))
            .build();

    assertEquals(
        expectedTransform,
        this.projectionTransformation
            .transform(originalRequest, mockTransformationContext)
            .blockingGet());
  }

  @Test
  void transformsGroupBys() {
    this.mockAttribute(PROJECTED_ATTRIBUTE_ID, this.attributeMetadata);
    this.mockAttribute(SIMPLE_ATTRIBUTE_ID, AttributeMetadata.getDefaultInstance());
    QueryRequest originalRequest =
        QueryRequest.newBuilder()
            .addAggregation(
                createFunctionExpression(
                    QUERY_FUNCTION_AVG,
                    createSimpleAttributeExpression(PROJECTED_ATTRIBUTE_ID).build()))
            .addGroupBy(createSimpleAttributeExpression(PROJECTED_ATTRIBUTE_ID))
            .build();
    QueryRequest expectedTransform =
        QueryRequest.newBuilder()
            .addAggregation(
                createFunctionExpression(
                    QUERY_FUNCTION_AVG,
                    createAliasedAttributeExpression(SIMPLE_ATTRIBUTE_ID, PROJECTED_ATTRIBUTE_ID)))
            .addGroupBy(
                createAliasedAttributeExpression(SIMPLE_ATTRIBUTE_ID, PROJECTED_ATTRIBUTE_ID))
            .build();

    assertEquals(
        expectedTransform,
        this.projectionTransformation
            .transform(originalRequest, mockTransformationContext)
            .blockingGet());
  }

  @Test
  void passesThroughExpressionsInOrder() {
    this.mockAttribute(PROJECTED_ATTRIBUTE_ID, this.attributeMetadata);
    this.mockAttribute(SIMPLE_ATTRIBUTE_ID, AttributeMetadata.getDefaultInstance());
    when(this.mockAttributeClient.get("slow"))
        .thenReturn(
            Single.defer(
                () ->
                    Single.just(AttributeMetadata.getDefaultInstance())
                        .delay(10, TimeUnit.MILLISECONDS)));

    QueryRequest originalRequest =
        QueryRequest.newBuilder()
            .addSelection(createSimpleAttributeExpression("slow"))
            .addSelection(createSimpleAttributeExpression(SIMPLE_ATTRIBUTE_ID))
            .addSelection(createSimpleAttributeExpression(PROJECTED_ATTRIBUTE_ID))
            .build();
    QueryRequest expected =
        QueryRequest.newBuilder()
            .addSelection(createSimpleAttributeExpression("slow"))
            .addSelection(createSimpleAttributeExpression(SIMPLE_ATTRIBUTE_ID))
            .addSelection(
                createAliasedAttributeExpression(SIMPLE_ATTRIBUTE_ID, PROJECTED_ATTRIBUTE_ID))
            .build();
    assertEquals(
        expected,
        this.projectionTransformation
            .transform(originalRequest, mockTransformationContext)
            .blockingGet());
  }

  @Test
  void passesThroughOrderBysInOrder() {
    this.mockAttribute(PROJECTED_ATTRIBUTE_ID, this.attributeMetadata);
    this.mockAttribute(SIMPLE_ATTRIBUTE_ID, AttributeMetadata.getDefaultInstance());
    when(this.mockAttributeClient.get("slow"))
        .thenReturn(
            Single.defer(
                () ->
                    Single.just(AttributeMetadata.getDefaultInstance())
                        .delay(10, TimeUnit.MILLISECONDS)));

    QueryRequest originalRequest =
        QueryRequest.newBuilder()
            .addOrderBy(
                createOrderByExpression(createSimpleAttributeExpression("slow"), SortOrder.ASC))
            .addOrderBy(
                createOrderByExpression(
                    createSimpleAttributeExpression(PROJECTED_ATTRIBUTE_ID), SortOrder.DESC))
            .build();
    QueryRequest expectedTransform =
        QueryRequest.newBuilder()
            .addOrderBy(
                createOrderByExpression(createSimpleAttributeExpression("slow"), SortOrder.ASC))
            .addOrderBy(
                createOrderByExpression(
                    createAliasedAttributeExpression(SIMPLE_ATTRIBUTE_ID, PROJECTED_ATTRIBUTE_ID)
                        .toBuilder(),
                    SortOrder.DESC))
            .build();

    assertEquals(
        expectedTransform,
        this.projectionTransformation
            .transform(originalRequest, mockTransformationContext)
            .blockingGet());
  }

  @Test
  void transformNullDefinedAttributes() {
    this.attributeMetadata =
        this.attributeMetadata.toBuilder()
            .setDefinition(AttributeDefinition.newBuilder().setProjection(nullLiteralProjection()))
            .setValueKind(AttributeKind.TYPE_STRING)
            .build();

    this.mockAttribute(PROJECTED_ATTRIBUTE_ID, this.attributeMetadata);
    this.mockAttribute(SIMPLE_ATTRIBUTE_ID, AttributeMetadata.getDefaultInstance());

    QueryRequest originalRequest =
        QueryRequest.newBuilder()
            .addSelection(createSimpleAttributeExpression(PROJECTED_ATTRIBUTE_ID))
            .addSelection(createSimpleAttributeExpression(SIMPLE_ATTRIBUTE_ID))
            .build();

    QueryRequest expectedTransform =
        QueryRequest.newBuilder()
            .addSelection(createNullStringLiteralExpression())
            .addSelection(createSimpleAttributeExpression(SIMPLE_ATTRIBUTE_ID))
            .build();

    assertEquals(
        expectedTransform,
        this.projectionTransformation
            .transform(originalRequest, mockTransformationContext)
            .blockingGet());

    // Now make it typed number
    this.attributeMetadata =
        this.attributeMetadata.toBuilder()
            .setDefinition(AttributeDefinition.newBuilder().setProjection(nullLiteralProjection()))
            .setValueKind(AttributeKind.TYPE_INT64)
            .build();

    this.mockAttribute(PROJECTED_ATTRIBUTE_ID, this.attributeMetadata);

    expectedTransform =
        QueryRequest.newBuilder()
            .addSelection(createNullNumberLiteralExpression())
            .addSelection(createSimpleAttributeExpression(SIMPLE_ATTRIBUTE_ID))
            .build();

    assertEquals(
        expectedTransform,
        this.projectionTransformation
            .transform(originalRequest, mockTransformationContext)
            .blockingGet());
  }

  @Test
  void transformsNullProjectionArguments() {
    // CONDITIONAL("true", SIMPLE_ATTRIBUTE_ID, "null")
    Projection projection =
        functionProjection(
            projectionExpression(
                PROJECTION_OPERATOR_CONDITIONAL,
                literalProjection("true"),
                attributeIdProjection(SIMPLE_ATTRIBUTE_ID),
                nullLiteralProjection()));

    this.attributeMetadata =
        this.attributeMetadata.toBuilder()
            .setDefinition(AttributeDefinition.newBuilder().setProjection(projection))
            .build();

    this.mockAttribute(PROJECTED_ATTRIBUTE_ID, this.attributeMetadata);
    this.mockAttribute(SIMPLE_ATTRIBUTE_ID, AttributeMetadata.getDefaultInstance());

    QueryRequest originalRequest =
        QueryRequest.newBuilder()
            .addSelection(createSimpleAttributeExpression(PROJECTED_ATTRIBUTE_ID))
            .build();

    QueryRequest expectedTransform =
        QueryRequest.newBuilder()
            .addSelection(
                createAliasedFunctionExpression(
                    QUERY_FUNCTION_CONDITIONAL,
                    PROJECTED_ATTRIBUTE_ID,
                    createStringLiteralValueExpression("true"),
                    createSimpleAttributeExpression(SIMPLE_ATTRIBUTE_ID).build(),
                    createNullStringLiteralExpression()))
            .build();

    assertEquals(
        expectedTransform,
        this.projectionTransformation
            .transform(originalRequest, mockTransformationContext)
            .blockingGet());
  }

  @Test
  void doesNotTransformMaterializedDefinition() {
    this.mockAttribute(
        PROJECTED_ATTRIBUTE_ID, this.attributeMetadata.toBuilder().setMaterialized(true).build());
    QueryRequest originalRequest =
        QueryRequest.newBuilder()
            .addSelection(createSimpleAttributeExpression(PROJECTED_ATTRIBUTE_ID))
            .build();

    assertEquals(
        originalRequest,
        this.projectionTransformation
            .transform(originalRequest, mockTransformationContext)
            .blockingGet());
  }

  private void mockAttribute(String id, AttributeMetadata attributeMetadata) {
    when(this.mockAttributeClient.get(id))
        .thenReturn(Single.defer(() -> Single.just(attributeMetadata)));
  }

  private ProjectionExpression projectionExpression(
      ProjectionOperator operator, Projection... arguments) {
    return ProjectionExpression.newBuilder()
        .setOperator(operator)
        .addAllArguments(asList(arguments))
        .build();
  }

  private Projection attributeIdProjection(String attributeId) {
    return Projection.newBuilder().setAttributeId(attributeId).build();
  }

  private Projection functionProjection(ProjectionExpression expression) {
    return Projection.newBuilder().setExpression(expression).build();
  }

  private Projection literalProjection(String value) {
    return Projection.newBuilder()
        .setLiteral(LiteralValue.newBuilder().setStringValue(value))
        .build();
  }

  private Projection nullLiteralProjection() {
    return Projection.newBuilder().setLiteral(LiteralValue.getDefaultInstance()).build();
  }
}
