package org.hypertrace.core.query.service;

import com.typesafe.config.Config;
import java.util.Set;
import org.hypertrace.core.query.service.api.QueryRequest;

public interface RequestHandler<T, R> {

  /** Get the name of Request Handler */
  String getName();

  QueryCost canHandle(T request, Set<String> referencedSources, ExecutionContext analyzer);

  /** Handle the request and add rows to the collector. */
  void handleRequest(ExecutionContext executionContext, QueryRequest request,
      QueryResultCollector<R> collector);

  void init(String name, Config config);
}
