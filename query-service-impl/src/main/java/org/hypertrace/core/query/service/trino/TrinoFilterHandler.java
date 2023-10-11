package org.hypertrace.core.query.service.trino;

import static org.hypertrace.core.query.service.QueryRequestUtil.getLogicalColumnName;
import static org.hypertrace.core.query.service.api.Expression.ValueCase.LITERAL;

import java.util.List;
import java.util.Optional;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ValueType;

public class TrinoFilterHandler {
  private static final String IS_TRINO_ATTRIBUTE = "Event.isTrino";

  public boolean containsAttributeFilter(QueryRequest request) {
    return request.hasFilter() && containsAttributeFilter(request.getFilter());
  }

  public Filter skipTrinoAttributeFilter(Filter filter) {
    return skipAttributeFilterIfPresent(filter);
  }

  private boolean containsAttributeFilter(Filter filter) {
    if (filter.getChildFilterCount() > 0) {
      for (Filter childFilter : filter.getChildFilterList()) {
        if (containsAttributeFilter(childFilter)) {
          return true;
        }
      }
    }
    return isTrinoAttributeFilter(filter);
  }

  private Filter skipAttributeFilterIfPresent(Filter filter) {
    if (filter.getChildFilterCount() > 0) {
      Filter.Builder builder = filter.toBuilder();
      List<Filter> childFilters = filter.getChildFilterList();
      builder.clearChildFilter();
      for (Filter childFilter : childFilters) {
        Filter skippedFilter = skipAttributeFilterIfPresent(childFilter);
        if (!skippedFilter.equals(Filter.getDefaultInstance())) {
          builder.addChildFilter(skippedFilter);
        }
      }
      if (builder.getChildFilterList().isEmpty()) {
        return Filter.getDefaultInstance();
      }
      return builder.build();
    } else if (isTrinoAttributeFilter(filter)) {
      return Filter.getDefaultInstance();
    }
    return filter;
  }

  private boolean isTrinoAttributeFilter(Filter filter) {
    // filter must be of the form: isTrino = true
    Optional<String> mayBeColumn = getLogicalColumnName(filter.getLhs());
    if (mayBeColumn.isPresent()) {
      if (mayBeColumn.get().equals(IS_TRINO_ATTRIBUTE)) {
        if (filter.getRhs().getValueCase() == LITERAL) {
          LiteralConstant value = filter.getRhs().getLiteral();
          if (value.getValue().getValueType() == ValueType.BOOL) {
            return value.getValue().getBoolean();
          }
        }
      }
    }
    return false;
  }
}
