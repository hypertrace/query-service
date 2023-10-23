package org.hypertrace.core.query.service.trino;

import static org.hypertrace.core.query.service.QueryRequestUtil.getLogicalColumnName;

import java.util.List;
import java.util.Optional;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.QueryRequest;

public class TrinoFilterHandler {
  public static final String IS_TRINO_EVENT_ATTRIBUTE = "EVENT.isTrino";
  public static final String IS_TRINO_API_TRACE_ATTRIBUTE = "API_TRACE.isTrino";

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
    // filter must contain Event.isTrino or API_TRACE.isTrino attribute
    Optional<String> mayBeColumn = getLogicalColumnName(filter.getLhs());
    return mayBeColumn.isPresent()
        && (mayBeColumn.get().equalsIgnoreCase(IS_TRINO_EVENT_ATTRIBUTE)
            || mayBeColumn.get().equalsIgnoreCase(IS_TRINO_API_TRACE_ATTRIBUTE));
  }
}
