package org.hypertrace.core.query.service;

import io.reactivex.rxjava3.core.Single;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.jetbrains.annotations.NotNull;

public interface QueryTransformation extends Comparable<QueryTransformation> {
  Single<QueryRequest> transform(
      QueryRequest queryRequest, QueryTransformationContext transformationContext);

  default int getPriority() {
    return 10;
  }

  @Override
  default int compareTo(@NotNull QueryTransformation other) {
    return Integer.compare(this.getPriority(), other.getPriority());
  }

  interface QueryTransformationContext {}
}
