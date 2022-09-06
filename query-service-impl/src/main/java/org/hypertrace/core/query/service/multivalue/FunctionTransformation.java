package org.hypertrace.core.query.service.multivalue;

import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_BOOL_ARRAY;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_DOUBLE_ARRAY;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_INT64_ARRAY;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_STRING_ARRAY;

import com.google.inject.Inject;
import io.reactivex.rxjava3.core.Single;
import java.util.List;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.query.service.api.Function;

public class FunctionTransformation {
  protected static final String DISTINCT_COUNT = "DISTINCTCOUNT";
  protected static final String DISTINCT_COUNT_MV = "DISTINCTCOUNTMV";
  private static final List<AttributeKind> ARRAY_KINDS =
      List.of(TYPE_STRING_ARRAY, TYPE_BOOL_ARRAY, TYPE_DOUBLE_ARRAY, TYPE_INT64_ARRAY);
  private final CachingAttributeClient attributeClient;

  @Inject
  public FunctionTransformation(CachingAttributeClient attributeClient) {
    this.attributeClient = attributeClient;
  }

  public Single<Function.Builder> transformFunction(Function.Builder builder) {
    switch (builder.getFunctionName()) {
      case DISTINCT_COUNT:
        // Checking if the function is of the form DISTINCT_COUNT(ATTRIBUTE_EXPRESSION)
        if (builder.getArgumentsList().size() == 1
            && builder.getArgumentsList().get(0).hasAttributeExpression()) {
          return transformDistinctCount(builder);
        }
        return Single.just(builder);
      default:
        return Single.just(builder);
    }
  }

  private Single<Function.Builder> transformDistinctCount(Function.Builder functionBuilder) {
    return this.getFirstAttributeId(functionBuilder)
        .flatMap(this.attributeClient::get)
        .map(
            metadata ->
                isArray(metadata.getValueKind())
                    ? functionBuilder.setFunctionName(DISTINCT_COUNT_MV)
                    : functionBuilder);
  }

  private boolean isArray(AttributeKind attributeKind) {
    return ARRAY_KINDS.contains(attributeKind);
  }

  private Single<String> getFirstAttributeId(Function.Builder functionBuilder) {
    return Single.just(functionBuilder.getArguments(0).getAttributeExpression().getAttributeId());
  }
}
