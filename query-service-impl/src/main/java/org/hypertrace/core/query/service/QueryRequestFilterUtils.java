package org.hypertrace.core.query.service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.query.service.api.Filter;

public class QueryRequestFilterUtils {
  public static Filter optimizeFilter(Filter filter) {
    // An optimization that can be done is to reduce the depth of the filter when the operation that's applied at
    // two consecutive levels of the filter tree is the same. For example, ((a = 1 OR b = 2) OR (d = 4 OR e = 5))
    // can be rewritten as (a = 1 OR b = 2 OR d = 4 OR e = 5). The same can be done with AND operation as well.
    if (filter.getChildFilterCount() > 0) {
      // This is a parent node. Optimize children separately and then optimize this node.
      List<Filter> optimizedChildren = filter.getChildFilterList().stream()
          .map(QueryRequestFilterUtils::optimizeFilter).collect(Collectors.toList());

      // Use a Set here to dedup the filters.
      Set<Filter> children = new HashSet<>();
      for (Filter child: optimizedChildren) {
        if (child.getChildFilterCount() > 0 && child.getOperator() != filter.getOperator()) {
          return filter;
        } else if (child.getChildFilterCount() > 0) {
          children.addAll(child.getChildFilterList());
        } else {
          children.add(child);
        }
      }

      // Create a new filter with all the leaf children and grand-children filters.
      return Filter.newBuilder().setOperator(filter.getOperator()).addAllChildFilter(children).build();
    } else {
      return filter;
    }
  }
}
