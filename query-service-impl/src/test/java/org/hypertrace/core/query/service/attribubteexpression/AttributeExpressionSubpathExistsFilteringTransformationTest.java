package org.hypertrace.core.query.service.attribubteexpression;

import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createColumnExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createComplexAttributeExpression;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createCompositeFilter;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createEqualsFilter;
import static org.hypertrace.core.query.service.QueryRequestBuilderUtils.createOrderByExpression;
import static org.hypertrace.core.query.service.QueryRequestUtil.createContainsKeyFilter;
import static org.hypertrace.core.query.service.QueryRequestUtil.createStringLiteralValueExpression;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.hypertrace.core.query.service.QueryTransformation.QueryTransformationContext;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.SortOrder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AttributeExpressionSubpathExistsFilteringTransformationTest {

  @Mock QueryTransformationContext mockTransformationContext;

  private final AttributeExpressionSubpathExistsFilteringTransformation transformation =
      new AttributeExpressionSubpathExistsFilteringTransformation();

  @Test
  void transQueryWithComplexAttributeExpression_SingleFilter() {
    Expression spanTags = createComplexAttributeExpression("Span.tags", "span.kind").build();
    Filter filter =
        Filter.newBuilder()
            .setLhs(spanTags)
            .setOperator(Operator.EQ)
            .setRhs(createColumnExpression("server"))
            .build();
    QueryRequest originalRequest = QueryRequest.newBuilder().setFilter(filter).build();

    QueryRequest expectedTransform =
        QueryRequest.newBuilder()
            .setFilter(
                Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addAllChildFilter(
                        List.of(filter, createContainsKeyFilter(spanTags.getAttributeExpression())))
                    .build())
            .build();

    assertEquals(
        expectedTransform,
        this.transformation.transform(originalRequest, mockTransformationContext).blockingGet());
  }

  @Test
  void transQueryWithComplexAttributeExpression_MultipleFilter() {
    Expression spanTags1 = createComplexAttributeExpression("Span.tags", "FLAGS").build();
    Expression spanTags2 = createComplexAttributeExpression("Span.tags", "span.kind").build();

    Filter childFilter1 =
        Filter.newBuilder()
            .setLhs(spanTags1)
            .setOperator(Operator.EQ)
            .setRhs(createStringLiteralValueExpression("0"))
            .build();
    Filter childFilter2 =
        Filter.newBuilder()
            .setLhs(spanTags2)
            .setOperator(Operator.EQ)
            .setRhs(createStringLiteralValueExpression("server"))
            .build();

    Filter.Builder filter = createCompositeFilter(Operator.AND, childFilter1, childFilter2);
    QueryRequest originalRequest = QueryRequest.newBuilder().setFilter(filter).build();

    QueryRequest expectedTransform =
        this.transformation.transform(originalRequest, mockTransformationContext).blockingGet();
    List<Filter> childFilterList = expectedTransform.getFilter().getChildFilterList();

    assertTrue(
        childFilterList
            .get(0)
            .getChildFilterList()
            .contains(createContainsKeyFilter(spanTags1.getAttributeExpression())));
    assertTrue(
        childFilterList
            .get(1)
            .getChildFilterList()
            .contains(createContainsKeyFilter(spanTags2.getAttributeExpression())));
  }

  @Test
  void transQueryWithEmptyFilter() {
    Filter emptyFilter = Filter.getDefaultInstance();
    QueryRequest originalRequest = QueryRequest.newBuilder().setFilter(emptyFilter).build();

    QueryRequest expectedTransform =
        this.transformation.transform(originalRequest, mockTransformationContext).blockingGet();
    assertFalse(expectedTransform.hasFilter());
  }

  @Test
  void transQueryWithComplexAttributeExpression_EmptyFilter() {
    Expression spanTags1 = createComplexAttributeExpression("Span.tags", "FLAGS").build();
    Expression spanTags2 = createComplexAttributeExpression("Span.tags", "span.kind").build();

    Filter childFilter1 =
        Filter.newBuilder()
            .setLhs(spanTags1)
            .setOperator(Operator.EQ)
            .setRhs(createStringLiteralValueExpression("0"))
            .build();
    Filter childFilter2 =
        Filter.newBuilder()
            .setLhs(spanTags2)
            .setOperator(Operator.EQ)
            .setRhs(createStringLiteralValueExpression("server"))
            .build();
    Filter emptyFilter = Filter.getDefaultInstance();

    Filter.Builder filter =
        createCompositeFilter(Operator.AND, childFilter1, childFilter2, emptyFilter);
    QueryRequest originalRequest = QueryRequest.newBuilder().setFilter(filter).build();

    QueryRequest expectedTransform =
        this.transformation.transform(originalRequest, mockTransformationContext).blockingGet();
    List<Filter> childFilterList = expectedTransform.getFilter().getChildFilterList();

    assertTrue(
        childFilterList
            .get(0)
            .getChildFilterList()
            .contains(createContainsKeyFilter(spanTags1.getAttributeExpression())));
    assertTrue(
        childFilterList
            .get(1)
            .getChildFilterList()
            .contains(createContainsKeyFilter(spanTags2.getAttributeExpression())));
  }

  @Test
  void transQueryWithComplexAttributeExpression_HierarchicalFilter() {
    Expression spanTags1 = createComplexAttributeExpression("Span.tags", "FLAGS").build();
    Expression spanTags2 = createComplexAttributeExpression("Span.tags", "span.kind").build();

    Filter filter1 =
        Filter.newBuilder()
            .setLhs(spanTags1)
            .setOperator(Operator.EQ)
            .setRhs(createStringLiteralValueExpression("0"))
            .build();
    Filter filter2 =
        Filter.newBuilder()
            .setLhs(spanTags2)
            .setOperator(Operator.EQ)
            .setRhs(createStringLiteralValueExpression("server"))
            .build();
    Filter filter =
        createCompositeFilter(
                Operator.AND,
                createEqualsFilter("other-attribute", "otherValue"),
                createCompositeFilter(Operator.AND, filter1, filter2).build())
            .build();

    QueryRequest originalRequest = QueryRequest.newBuilder().setFilter(filter).build();

    QueryRequest expectedTransform =
        this.transformation.transform(originalRequest, mockTransformationContext).blockingGet();
    List<Filter> childFilterList =
        expectedTransform.getFilter().getChildFilterList().get(1).getChildFilterList();

    assertTrue(
        childFilterList
            .get(0)
            .getChildFilterList()
            .contains(createContainsKeyFilter(spanTags1.getAttributeExpression())));
    assertTrue(
        childFilterList
            .get(1)
            .getChildFilterList()
            .contains(createContainsKeyFilter(spanTags2.getAttributeExpression())));
  }

  @Test
  void transQueryWithComplexAttributeExpression_OrderByAndFilter() {
    Expression.Builder spanTag = createComplexAttributeExpression("Span.tags", "span.kind");

    Filter filter =
        Filter.newBuilder()
            .setLhs(spanTag)
            .setOperator(Operator.EQ)
            .setRhs(createStringLiteralValueExpression("server"))
            .build();
    Filter containsKeyFilter = createContainsKeyFilter("Span.tags", "span.kind");

    QueryRequest originalRequest =
        QueryRequest.newBuilder()
            .setFilter(filter)
            .addOrderBy(createOrderByExpression(spanTag, SortOrder.ASC))
            .build();

    QueryRequest expectedTransform =
        QueryRequest.newBuilder()
            .setFilter(
                createCompositeFilter(
                    Operator.AND,
                    createCompositeFilter(Operator.AND, filter, containsKeyFilter).build(),
                    containsKeyFilter))
            .addOrderBy(createOrderByExpression(spanTag, SortOrder.ASC))
            .build();

    assertEquals(
        expectedTransform,
        this.transformation.transform(originalRequest, mockTransformationContext).blockingGet());
  }

  @Test
  void transQueryWithComplexAttributeExpression_SingleSelection() {
    Expression.Builder spanTag = createComplexAttributeExpression("Span.tags", "span.kind");

    QueryRequest originalRequest = QueryRequest.newBuilder().addSelection(spanTag).build();

    QueryRequest expectedTransform =
        QueryRequest.newBuilder()
            .addSelection(spanTag)
            .setFilter(createContainsKeyFilter("Span.tags", "span.kind"))
            .build();

    assertEquals(
        expectedTransform,
        this.transformation.transform(originalRequest, mockTransformationContext).blockingGet());
  }

  @Test
  void transQueryWithComplexAttributeExpression_SingleOrderBy() {
    Expression.Builder spanTag = createComplexAttributeExpression("Span.tags", "span.kind");

    QueryRequest originalRequest =
        QueryRequest.newBuilder()
            .addOrderBy(createOrderByExpression(spanTag, SortOrder.ASC))
            .build();

    QueryRequest expectedTransform =
        QueryRequest.newBuilder()
            .setFilter(createContainsKeyFilter("Span.tags", "span.kind"))
            .addOrderBy(createOrderByExpression(spanTag, SortOrder.ASC))
            .build();

    assertEquals(
        expectedTransform,
        this.transformation.transform(originalRequest, mockTransformationContext).blockingGet());
  }

  @Test
  void transQueryWithComplexAttributeExpression_MultipleOrderBy() {
    Expression.Builder spanTag1 = createComplexAttributeExpression("Span.tags", "span.kind");
    Expression.Builder spanTag2 = createComplexAttributeExpression("Span.tags", "FLAGS");

    QueryRequest originalRequest =
        QueryRequest.newBuilder()
            .addOrderBy(createOrderByExpression(spanTag1, SortOrder.ASC))
            .addOrderBy(createOrderByExpression(spanTag2, SortOrder.ASC))
            .build();

    QueryRequest expectedTransform =
        QueryRequest.newBuilder()
            .setFilter(
                createCompositeFilter(
                    Operator.AND,
                    createContainsKeyFilter("Span.tags", "span.kind"),
                    createContainsKeyFilter("Span.tags", "FLAGS")))
            .addOrderBy(createOrderByExpression(spanTag1, SortOrder.ASC))
            .addOrderBy(createOrderByExpression(spanTag2, SortOrder.ASC))
            .build();

    assertEquals(
        expectedTransform,
        this.transformation.transform(originalRequest, mockTransformationContext).blockingGet());
  }
}
