package org.hypertrace.core.query.service.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.hypertrace.core.query.service.QueryServiceConfig.RequestHandlerClientConfig;

/*
 * Factory to create PostgresClient based on postgres jdbc connection.
 */
public class PostgresClientFactory {

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(PostgresClientFactory.class);
  private static final Integer DEFAULT_MAX_CONNECTIONS = 5;
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

  public static PostgresClient createPostgresClient(DataSource dataSource) {
    return new PostgresClient(dataSource);
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

    private final DataSource dataSource;

    private PostgresClient(RequestHandlerClientConfig clientConfig) throws SQLException {
      String user = clientConfig.getUser().orElseThrow(IllegalArgumentException::new);
      String password = clientConfig.getPassword().orElseThrow(IllegalArgumentException::new);
      String url = clientConfig.getConnectionString();
      int maxConnections = clientConfig.getMaxConnections().orElse(DEFAULT_MAX_CONNECTIONS);
      this.dataSource = createPooledDataSource(url, user, password, maxConnections);
    }

    private DataSource createPooledDataSource(
        String url, String user, String password, int maxConnections) {
      LOG.debug(
          "Trying to create a Postgres client connected to postgres server using url: {}, user: {}",
          url,
          user);
      ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(url, user, password);
      PoolableConnectionFactory poolableConnectionFactory =
          new PoolableConnectionFactory(connectionFactory, null);
      GenericObjectPool<PoolableConnection> connectionPool =
          new GenericObjectPool<>(poolableConnectionFactory);
      connectionPool.setMaxTotal(maxConnections);
      // max idle connections are 20% of max connections
      connectionPool.setMaxIdle(getPercentOf(maxConnections, 20));
      // min idle connections are 10% of max connections
      connectionPool.setMinIdle(getPercentOf(maxConnections, 10));
      connectionPool.setBlockWhenExhausted(true);
      connectionPool.setMaxWaitMillis(5000);
      poolableConnectionFactory.setPool(connectionPool);
      poolableConnectionFactory.setValidationQuery("SELECT 1");
      poolableConnectionFactory.setValidationQueryTimeout(5);
      poolableConnectionFactory.setDefaultReadOnly(false);
      poolableConnectionFactory.setDefaultAutoCommit(false);
      poolableConnectionFactory.setDefaultTransactionIsolation(
          Connection.TRANSACTION_READ_COMMITTED);
      poolableConnectionFactory.setPoolStatements(false);
      return new PoolingDataSource<>(connectionPool);
    }

    private PostgresClient(DataSource dataSource) {
      this.dataSource = dataSource;
    }

    public Connection getConnection() throws SQLException {
      return dataSource.getConnection();
    }

    private int getPercentOf(int maxConnections, int percent) {
      int value = (maxConnections * percent) / 100;
      // minimum value should be 1
      return Math.max(value, 1);
    }
  }
}
