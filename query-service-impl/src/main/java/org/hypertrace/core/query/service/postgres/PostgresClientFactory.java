package org.hypertrace.core.query.service.postgres;

import com.google.common.annotations.VisibleForTesting;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.codec.binary.Hex;

/*
 * Factory to create PostgresClient based on given zookeeper path.
 */
public class PostgresClientFactory {

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(PostgresClientFactory.class);
  // Singleton instance
  private static final PostgresClientFactory INSTANCE = new PostgresClientFactory();

  private final ConcurrentHashMap<String, PostgresClient> clientMap = new ConcurrentHashMap<>();

  private PostgresClientFactory() {}

  // Create a Postgres Client.
  public static PostgresClient createPostgresClient(String postgresCluster, String path)
      throws SQLException {
    if (!get().containsClient(postgresCluster)) {
      synchronized (get()) {
        if (!get().containsClient(postgresCluster)) {
          get().addPostgresClient(postgresCluster, new PostgresClient(path));
        }
      }
    }
    return get().getPostgresClient(postgresCluster);
  }

  public static PostgresClient createPostgresClient(Connection connection) {
    return new PostgresClient(connection);
  }

  public static PostgresClientFactory get() {
    return INSTANCE;
  }

  private void addPostgresClient(String cluster, PostgresClient client) {
    this.clientMap.put(cluster, client);
  }

  public boolean containsClient(String clusterName) {
    return this.clientMap.containsKey(clusterName);
  }

  public PostgresClient getPostgresClient(String clusterName) {
    return this.clientMap.get(clusterName);
  }

  public static class PostgresClient {

    private final Connection connection;

    private PostgresClient(String url) throws SQLException {
      LOG.info(
          "Trying to create a Postgres client connected to postgres server using url: {}", url);
      this.connection = DriverManager.getConnection(url);
    }

    private PostgresClient(Connection connection) {
      this.connection = connection;
    }

    public ResultSet executeQuery(String statement, Params params) throws SQLException {
      PreparedStatement preparedStatement = buildPreparedStatement(statement, params);
      return preparedStatement.executeQuery();
    }

    private PreparedStatement buildPreparedStatement(String statement, Params params)
        throws SQLException {
      String resolvedStatement = resolveStatement(statement, params);
      return connection.prepareStatement(resolvedStatement);
    }

    @VisibleForTesting
    /*
     * Postgres PreparedStatement creates invalid query if one of the parameters has '?' in its value.
     * Sample postgres query : select * from table where team in (?, ?, ?).. Now say parameters are:
     * 'abc', 'pqr with (?)' and 'xyz'..
     *
     * Now, on executing PreparedStatement:fillStatementWithParameters method on this will return
     * select * from table where team in ('abc', 'pqr with ('xyz')', ?) -- which is clearly wrong
     * (what we wanted was select * from table where team in ('abc', 'pqr with (?)', 'xyz'))..
     *
     * The reason is the usage of replaceFirst iteration in the postgres PreparedStatement method..
     *
     * Hence written this custom method to resolve the query statement rather than relying on Postgres's
     * library method.
     * This is a temporary fix and can be reverted when the Postgres issue gets resolved
     * Raised an issue in incubator-postgres github repo: apache/incubator-postgres#6834
     */
    static String resolveStatement(String query, Params params) {
      if (query.isEmpty()) {
        return query;
      }
      String[] queryParts = query.split("\\?");

      String[] parameters = new String[queryParts.length];
      params.getStringParams().forEach((i, p) -> parameters[i] = getStringParam(p));
      params.getIntegerParams().forEach((i, p) -> parameters[i] = String.valueOf(p));
      params.getLongParams().forEach((i, p) -> parameters[i] = String.valueOf(p));
      params.getDoubleParams().forEach((i, p) -> parameters[i] = String.valueOf(p));
      params.getFloatParams().forEach((i, p) -> parameters[i] = String.valueOf(p));
      params
          .getByteStringParams()
          .forEach((i, p) -> parameters[i] = getStringParam(Hex.encodeHexString(p.toByteArray())));

      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < queryParts.length; i++) {
        sb.append(queryParts[i]);
        sb.append(parameters[i] != null ? parameters[i] : "");
      }
      String statement = sb.toString();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Resolved SQL statement: [{}]", statement);
      }
      return statement;
    }

    private static String getStringParam(String value) {
      return "'" + value.replace("'", "''") + "'";
    }
  }
}
