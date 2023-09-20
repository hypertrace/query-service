package org.hypertrace.core.query.service.trino;

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
 * Factory to create TrinoClient based on Trino jdbc connection.
 */
public class TrinoClientFactory {
  private static final int DEFAULT_MAX_CONNECTION_ATTEMPTS = 200;
  private static final Duration DEFAULT_CONNECTION_RETRY_BACKOFF = Duration.ofSeconds(5);
  private static final int VALIDATION_QUERY_TIMEOUT_SECONDS = 5;

  // Singleton instance
  private static final TrinoClientFactory INSTANCE = new TrinoClientFactory();

  private final ConcurrentHashMap<String, TrinoClient> clientMap = new ConcurrentHashMap<>();

  private TrinoClientFactory() {}

  // Create a Trino Client
  public static TrinoClient createTrinoClient(
      String trinoCluster, RequestHandlerClientConfig clientConfig) throws SQLException {
    if (!get().containsClient(trinoCluster)) {
      synchronized (get()) {
        if (!get().containsClient(trinoCluster)) {
          get().addTrinoClient(trinoCluster, new TrinoClient(clientConfig));
        }
      }
    }
    return get().getTrinoClient(trinoCluster);
  }

  public static TrinoClientFactory get() {
    return INSTANCE;
  }

  public boolean containsClient(String clusterName) {
    return this.clientMap.containsKey(clusterName);
  }

  public TrinoClient getTrinoClient(String clusterName) {
    return this.clientMap.get(clusterName);
  }

  private void addTrinoClient(String cluster, TrinoClient client) {
    this.clientMap.put(cluster, client);
  }

  public static class TrinoClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(TrinoClient.class);

    private final String url;
    private final String user;
    private final String password;
    private final int maxConnectionAttempts;
    private final Duration connectionRetryBackoff;

    private int count = 0;
    private Connection connection;

    public TrinoClient(RequestHandlerClientConfig clientConfig) {
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
          LOGGER.info("Trino connection is invalid. Reconnecting...");
          close();
          newConnection();
        }
      } catch (SQLException sqle) {
        throw new RuntimeException(sqle);
      }
      return connection;
    }

    private boolean isConnectionValid(Connection connection) {
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

    private void newConnection() throws SQLException {
      ++count;
      int attempts = 0;
      while (attempts < maxConnectionAttempts) {
        try {
          ++attempts;
          LOGGER.info("Attempting(attempt #{}) to open connection #{} to {}", attempts, count, url);
          connection = DriverManager.getConnection(url, user, password);
          return;
        } catch (SQLException sqle) {
          attempts++;
          if (attempts < maxConnectionAttempts) {
            LOGGER.info(
                "Unable to connect(#{}) to database on attempt {}/{}. Will retry in {} ms.",
                count,
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

    private void close() {
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
