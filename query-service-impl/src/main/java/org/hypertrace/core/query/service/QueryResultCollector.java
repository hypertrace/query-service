package org.hypertrace.core.query.service;

/** Interface which is passed as a callback to {@link RequestHandler} */
public interface QueryResultCollector<T> {

  /**
   * Collect and handle the response received in T.
   *
   * @param t One of the items in the result.
   */
  void collect(T t);

  /** Finish collecting all the results and wrap up the query. */
  void finish();
}
