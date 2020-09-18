package org.hypertrace.core.query.service;

import com.typesafe.config.Config;
import java.util.Set;
import org.hypertrace.core.query.service.api.QueryRequest;

/**
 * Interface to be implemented by the different handlers which will handle the queries coming
 * into the query service. We expect different implementations for different data stores that
 * could have data which is served by the query service.
 *
 * <p>The callers of this will be first checking if the handler can handle the given query,
 * and can decide whether they want this handler to handle the query.
 *
 * QueryCost cost = handler.canHandle(....);
 *
 * // If the caller decides to use this handler
 * handler.handleRequest(....);
 *
 * </p>
 */
public interface RequestHandler<T, R> {

  String getName();

  QueryCost canHandle(T request, ExecutionContext context);

  /**
   * Handle the request and add rows to the collector.
   */
  void handleRequest(ExecutionContext executionContext, QueryRequest request,
      QueryResultCollector<R> collector);
}
