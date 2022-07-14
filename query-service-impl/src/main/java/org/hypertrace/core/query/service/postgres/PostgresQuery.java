package org.hypertrace.core.query.service.postgres;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import org.hypertrace.core.query.service.postgres.PostgresClientFactory.PostgresClient;

/*
 * PostgresQuery provides basic interface for getting query response from Postgres.
 */
public abstract class PostgresQuery<T> {

  private final String name;
  private final PostgresClient postgresClient;

  public PostgresQuery(String name, PostgresClient postgresClient) {
    this.name = name;
    this.postgresClient = postgresClient;
  }

  public String getName() {
    return this.name;
  }

  abstract String getQuery(Map<String, Object> args);

  abstract T convertQueryResults(ResultSet queryResults);

  public T execute() throws SQLException {
    return execute(Collections.emptyMap());
  }

  public T execute(Map<String, Object> args) throws SQLException {
    final ResultSet queryResults =
        this.postgresClient.executeQuery(getQuery(args), Params.newBuilder().build());
    return convertQueryResults(queryResults);
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 31 * hash + (name == null ? 0 : name.hashCode());
    hash = 31 * hash + (postgresClient == null ? 0 : postgresClient.hashCode());
    return hash;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (this.getClass() != o.getClass()) {
      return false;
    }
    PostgresQuery pq = (PostgresQuery) o;
    return (this.name.equals(pq.name) && this.postgresClient == pq.postgresClient);
  }
}
