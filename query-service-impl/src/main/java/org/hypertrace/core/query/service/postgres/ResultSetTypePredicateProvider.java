package org.hypertrace.core.query.service.postgres;

import java.sql.ResultSet;

/**
 * This interface is used to determine which handler will parse the Postgres ResultSet in
 * PostgresBasedRequestHandler#convert(). We define it to make it easy to unit test the parsing
 * logic since: - The implementations of ResultSet are package private and there's no way to
 * determine the concrete type of the ResultSet object other than using the class name. See
 * DefaultResultSetTypePredicateProvider class. - The ResultSet interface itself is implemented non
 * uniformly by its implementations. The defined methods in the interface do not return consistent
 * data across the implementations and the format of the implementations is different. - However,
 * since it seems like for "sql" format the ResultTableResultSet is being returned for all Postgres
 * query types we might be able to get rid of this in the future and have a single flow to parse the
 * Postgres Response.
 */
public interface ResultSetTypePredicateProvider {
  boolean isSelectionResultSetType(ResultSet resultSet);

  boolean isResultTableResultSetType(ResultSet resultSet);
}
