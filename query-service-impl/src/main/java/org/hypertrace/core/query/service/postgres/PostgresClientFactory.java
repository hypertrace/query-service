package org.hypertrace.core.query.service.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.reactivex.rxjava3.core.Observable;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import lombok.SneakyThrows;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.hypertrace.core.query.service.QueryServiceConfig.RequestHandlerClientConfig;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;

/*
 * Factory to create PostgresClient based on postgres jdbc connection.
 */
public class PostgresClientFactory {

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(PostgresClientFactory.class);
  // Singleton instance
  private static final PostgresClientFactory INSTANCE = new PostgresClientFactory();

  private static final Value NULL_VALUE =
      Value.newBuilder().setValueType(ValueType.STRING).setString("null").build();

  private static final ObjectMapper MAPPER = new ObjectMapper();

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
      this.dataSource = createPooledDataSource(url, user, password);
    }

    private DataSource createPooledDataSource(String url, String user, String password) {
      LOG.debug(
          "Trying to create a Postgres client connected to postgres server using url: {}, user: {}, password: {}",
          url,
          user,
          password);
      ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(url, user, password);
      PoolableConnectionFactory poolableConnectionFactory =
          new PoolableConnectionFactory(connectionFactory, null);
      GenericObjectPool<PoolableConnection> connectionPool =
          new GenericObjectPool<>(poolableConnectionFactory);
      connectionPool.setMaxTotal(50);
      connectionPool.setMaxIdle(10);
      connectionPool.setMinIdle(5);
      connectionPool.setMaxWaitMillis(5000);
      poolableConnectionFactory.setPool(connectionPool);
      poolableConnectionFactory.setValidationQuery("SELECT 1");
      poolableConnectionFactory.setValidationQueryTimeout(5);
      poolableConnectionFactory.setDefaultReadOnly(false);
      poolableConnectionFactory.setDefaultAutoCommit(false);
      poolableConnectionFactory.setDefaultTransactionIsolation(
          Connection.TRANSACTION_READ_COMMITTED);
      poolableConnectionFactory.setPoolStatements(false);
      // poolableConnectionFactory.setMaxOpenPreparedStatements(100);
      return new PoolingDataSource<>(connectionPool);
    }

    private PostgresClient(DataSource dataSource) {
      this.dataSource = dataSource;
    }

    public Observable<Row> executeQuery(String statement, Params params) throws SQLException {
      String resolvedStatement = resolveStatement(statement, params);
      try (Connection connection = dataSource.getConnection();
          PreparedStatement preparedStatement = connection.prepareStatement(resolvedStatement);
          ResultSet resultSet = preparedStatement.executeQuery()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Query results: [ {} ]", resultSet);
        }
        return convert(resultSet);
      }
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

    @SneakyThrows
    Observable<Row> convert(ResultSet resultSet) {
      List<Row.Builder> rowBuilderList = new ArrayList<>();
      while (resultSet.next()) {
        Row.Builder builder = Row.newBuilder();
        rowBuilderList.add(builder);
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        if (columnCount > 0) {
          for (int c = 1; c <= columnCount; c++) {
            int colType = metaData.getColumnType(c);
            Value convertedColVal;
            if (colType == Types.ARRAY) {
              Array colVal = resultSet.getArray(c);
              convertedColVal =
                  Value.newBuilder()
                      .setValueType(ValueType.STRING)
                      .setString(
                          MAPPER.writeValueAsString(
                              colVal != null ? colVal.getArray() : Collections.emptyList()))
                      .build();
            } else {
              String colVal = resultSet.getString(c);
              convertedColVal =
                  colVal != null
                      ? Value.newBuilder().setValueType(ValueType.STRING).setString(colVal).build()
                      : NULL_VALUE;
            }
            builder.addColumn(convertedColVal);
          }
        }
      }
      return Observable.fromIterable(rowBuilderList)
          .map(Row.Builder::build)
          .doOnNext(row -> LOG.debug("collect a row: {}", row));
    }
  }
}
