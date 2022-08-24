package org.hypertrace.core.query.service.postgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.query.service.QueryServiceConfig.RequestHandlerClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Factory to create PostgresClient based on postgres jdbc connection.
 */
public class PostgresClientFactory {

  private static final int DEFAULT_MAX_CONNECTION_ATTEMPTS = 200;
  private static final Duration DEFAULT_CONNECTION_RETRY_BACKOFF = Duration.ofSeconds(5);
  private static final int VALIDATION_QUERY_TIMEOUT_SECONDS = 5;

  // Singleton instance
  private static final PostgresClientFactory INSTANCE = new PostgresClientFactory();

  private final ConcurrentHashMap<String, PostgresClient> clientMap = new ConcurrentHashMap<>();

  private PostgresClientFactory() {}

  // Create a Postgres Client.
  public static PostgresClient createPostgresClient(
      String postgresCluster, RequestHandlerClientConfig clientConfig) throws SQLException {
    if (!get().containsClient(postgresCluster)) {
      synchronized (get()) {
        if (!get().containsClient(postgresCluster)) {
          get().addPostgresClient(postgresCluster, new PostgresClient(clientConfig));
        }
      }
    }
    return get().getPostgresClient(postgresCluster);
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

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresClient.class);

    private final String url;
    private final String user;
    private final String password;
    private final int maxConnectionAttempts;
    private final Duration connectionRetryBackoff;

    private int count = 0;
    private Connection connection;

    public PostgresClient(RequestHandlerClientConfig clientConfig) {
      this.url = clientConfig.getConnectionString();
      this.user = clientConfig.getUser().orElseThrow(IllegalArgumentException::new);
      this.password = clientConfig.getPassword().orElseThrow(IllegalArgumentException::new);
      this.maxConnectionAttempts =
          clientConfig.getMaxConnectionAttempts().orElse(DEFAULT_MAX_CONNECTION_ATTEMPTS);
      this.connectionRetryBackoff =
          clientConfig.getConnectionRetryBackoff().orElse(DEFAULT_CONNECTION_RETRY_BACKOFF);
    }

    public synchronized Connection getConnection() {
      try {
        if (connection == null) {
          newConnection();
        } else if (!isConnectionValid(connection)) {
          LOGGER.info("The database connection is invalid. Reconnecting...");
          close();
          newConnection();
        }
      } catch (SQLException sqle) {
        throw new RuntimeException(sqle);
      }
      return connection;
    }

    private synchronized boolean isConnectionValid(Connection connection) {
      try {
        if (connection.getMetaData().getJDBCMajorVersion() >= 4) {
          return connection.isValid(VALIDATION_QUERY_TIMEOUT_SECONDS);
        } else {
          try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT 1");
              ResultSet resultSet = preparedStatement.executeQuery()) {
            return true;
          }
        }
      } catch (SQLException sqle) {
        LOGGER.debug("Unable to check if the underlying connection is valid", sqle);
        return false;
      }
    }

    private synchronized void newConnection() throws SQLException {
      int attempts = 0;
      while (attempts < maxConnectionAttempts) {
        try {
          ++count;
          LOGGER.info("Attempting to open connection #{} to {}", count, url);
          connection = DriverManager.getConnection(url, user, password);
          return;
        } catch (SQLException sqle) {
          attempts++;
          if (attempts < maxConnectionAttempts) {
            LOGGER.info(
                "Unable to connect to database on attempt {}/{}. Will retry in {} ms.",
                attempts,
                maxConnectionAttempts,
                connectionRetryBackoff,
                sqle);
            try {
              TimeUnit.MILLISECONDS.sleep(connectionRetryBackoff.toMillis());
            } catch (InterruptedException e) {
              // this is ok because just woke up early
            }
          } else {
            throw sqle;
          }
        }
      }
    }

    private synchronized void close() {
      if (connection != null) {
        try {
          LOGGER.info("Closing connection #{} to {}", count, url);
          connection.close();
        } catch (SQLException sqle) {
          LOGGER.warn("Ignoring error closing connection", sqle);
        } finally {
          connection = null;
        }
      }
    }
  }
}
