package org.hypertrace.core.query.service;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Map;
import java.util.Set;
import org.hypertrace.core.query.service.api.QueryRequest;

public interface RequestHandler<T, R> {

  /** Get the name of Request Handler */
  String getName();

  QueryCost canHandle(T request, Set<String> referencedSources, Set<String> referencedColumns);

  /** Handle the request and add rows to the collector. */
  void handleRequest(
      QueryContext queryContext,
      QueryRequest request,
      QueryResultCollector<R> collector,
      RequestAnalyzer requestAnalyzer) throws InvalidProtocolBufferException;

  void init(String name, Map<String, Object> config);
}
