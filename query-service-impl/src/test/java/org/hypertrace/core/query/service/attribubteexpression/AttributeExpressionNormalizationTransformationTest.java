package org.hypertrace.core.query.service.attribubteexpression;

import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createAliasedAttributeExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createAliasedColumnExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createAliasedFunctionExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createColumnExpression;
import static org.hypertrace.core.query.service.QueryRequestUtil.createSimpleAttributeExpression;
import static org.hypertrace.core.query.service.QueryRequestUtil.createStringLiteralValueExpression;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hypertrace.core.query.service.QueryFunctionConstants;
import org.hypertrace.core.query.service.QueryTransformation.QueryTransformationContext;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.OrderByExpression;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AttributeExpressionNormalizationTransformationTest {

  @Mock QueryTransformationContext mockTransformationContext;

  private final AttributeExpressionNormalizationTransformation transformation =
      new AttributeExpressionNormalizationTransformation();

  @Test
  void columnInAllPositions() {
    QueryRequest originalRequest =
        QueryRequest.newBuilder()
            .addSelection(createAliasedColumnExpression("select-col", "select-alias"))
            .addAggregation(
                createAliasedFunctionExpression(
                    QueryFunctionConstants.QUERY_FUNCTION_SUM,
                    "agg-alias",
                    createColumnExpression("agg-col").build()))
            .setFilter(
                Filter.newBuilder()
                    .setLhs(createAliasedColumnExpression("filter-col", "filter-alias"))
                    .setOperator(Operator.EQ)
                    .setRhs(createStringLiteralValueExpression("val")))
            .addOrderBy(
                OrderByExpression.newBuilder()
                    .setExpression(createAliasedColumnExpression("orderby-col", "orderby-alias")))
            .addGroupBy(createAliasedColumnExpression("groupby-col", "groupby-alias"))
            .build();

    QueryRequest expectedTransform =
        QueryRequest.newBuilder()
            .addSelection(createAliasedAttributeExpression("select-col", "select-alias"))
            .addAggregation(
                createAliasedFunctionExpression(
                    QueryFunctionConstants.QUERY_FUNCTION_SUM,
                    "agg-alias",
                    createSimpleAttributeExpression("agg-col").build()))
            .setFilter(
                Filter.newBuilder()
                    .setLhs(createAliasedAttributeExpression("filter-col", "filter-alias"))
                    .setOperator(Operator.EQ)
                    .setRhs(createStringLiteralValueExpression("val")))
            .addOrderBy(
                OrderByExpression.newBuilder()
                    .setExpression(
                        createAliasedAttributeExpression("orderby-col", "orderby-alias")))
            .addGroupBy(createAliasedAttributeExpression("groupby-col", "groupby-alias"))
            .build();

    assertEquals(
        expectedTransform,
        this.transformation.transform(originalRequest, mockTransformationContext).blockingGet());
  }
}
