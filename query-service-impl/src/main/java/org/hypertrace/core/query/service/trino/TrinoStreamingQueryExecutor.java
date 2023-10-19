package org.hypertrace.core.query.service.trino;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.SneakyThrows;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.Row.Builder;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrinoStreamingQueryExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(TrinoStreamingQueryExecutor.class);
  private static final Value NULL_STRING_EQ_STRING_VALUE =
      Value.newBuilder().setValueType(ValueType.STRING).setString("null").build();
  private static final Value NULL_INTEGER_EQ_STRING_VALUE =
      Value.newBuilder().setValueType(ValueType.STRING).setString("0").build();
  private static final Value NULL_FLOAT_EQ_STRING_VALUE =
      Value.newBuilder().setValueType(ValueType.STRING).setString("0.0").build();
  private static final Value NULL_BOOLEAN_EQ_STRING_VALUE =
      Value.newBuilder().setValueType(ValueType.STRING).setString("false").build();
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ExecutorService executorService;
  private List<Row> rows = new ArrayList<>();

  public TrinoStreamingQueryExecutor() {
    this.executorService = Executors.newFixedThreadPool(10);
  }

  public Observable<Row> executeStreamingQuerySequential(String statement, Connection connection) throws SQLException {
    List<String> queries = getSubQueries(statement);
    for (String query : queries) {
      executeSubQuery(query, connection);
    }
    // consume the result rows
    return Observable.fromIterable(rows).doOnNext(row -> LOG.debug("collect a row: {}", row));
  }

  public Observable<Row> executeStreamingQueryParallel(String statement, Connection connection) throws SQLException {
    List<Future<?>> futures = new ArrayList<>();
    List<String> queries = getSubQueries(statement);
    int sleepTime = 0;
    for (String query : queries) {
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException ignored) {}
      futures.add(executorService.submit(() -> executeSubQuery(query, connection)));
      //sleepTime+= 10000;
    }
    futures.forEach(future -> waitForCompletion(future, 600*1000L));

    // consume the result rows
    return Observable.fromIterable(rows).doOnNext(row -> LOG.debug("collect a row: {}", row));
  }

  private List<String> getSubQueries(String statement) {
    List<String> queries = new ArrayList<>();

    String query1 = "SELECT count(*)"
        + " FROM span_event_view_staging_test "
        + "where customer_id = 'b227d0f9-98e1-4eff-acf5-ab129d416914' "
        + "and start_time_millis >= 1695081600000  and start_time_millis < 1695103200000 "
        + "AND is_bare != true AND environment = 'production' AND api_boundary_type = 'ENTRY' "
        + "AND api_id != 'null' AND regexp_like(request_body, '.*id.*') limit 1000";

    String query2 = "SELECT count(*)"
        + " FROM span_event_view_staging_test "
        + "where customer_id = 'b227d0f9-98e1-4eff-acf5-ab129d416914' "
        + "and start_time_millis >= 1695103200000  and start_time_millis < 1695124800000 "
        + "AND is_bare != true AND environment = 'production' AND api_boundary_type = 'ENTRY' "
        + "AND api_id != 'null' AND regexp_like(request_body, '.*id.*') limit 1000";

    String query3 = "SELECT count(*)"
        + " FROM span_event_view_staging_test "
        + "where customer_id = 'b227d0f9-98e1-4eff-acf5-ab129d416914' "
        + "and start_time_millis >= 1695124800000  and start_time_millis < 1695146400000 "
        + "AND is_bare != true AND environment = 'production' AND api_boundary_type = 'ENTRY' "
        + "AND api_id != 'null' AND regexp_like(request_body, '.*id.*') limit 1000";

    String query4 = "SELECT count(*)"
        + " FROM span_event_view_staging_test "
        + "where customer_id = 'b227d0f9-98e1-4eff-acf5-ab129d416914' "
        + "and start_time_millis >= 1695146400000  and start_time_millis < 1695168000000 "
        + "AND is_bare != true AND environment = 'production' AND api_boundary_type = 'ENTRY' "
        + "AND api_id != 'null' AND regexp_like(request_body, '.*id.*') limit 1000";

    queries.add(query1);
    queries.add(query2);
    queries.add(query3);
    queries.add(query4);
    return queries;
  }

  private void executeSubQuery(String statement, Connection connection) {
    LOG.info("executing query: " + statement);
    long startTimeMillis = System.currentTimeMillis();
    try (PreparedStatement preparedStatement = connection.prepareStatement(statement);
        ResultSet resultSet = preparedStatement.executeQuery()) {
      LOG.debug("Query results: [ {} ]", resultSet);

      convert(resultSet);
      long endTimeMillis = System.currentTimeMillis();
      LOG.info("converted result in seconds: " + (endTimeMillis - startTimeMillis)/1000
          + " for query: " + statement);
    } catch (Exception ex) {
      // Catch this exception to log the Trino SQL query that caused the issue
      LOG.error("An error occurred while executing: {}", statement, ex);
      // Rethrow for the caller to return an error.
      throw new RuntimeException(ex);
    }
  }

  private void waitForCompletion(Future<?> future, long timeoutInMillis) {
    try {
      future.get(timeoutInMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.error("Interrupted while waiting to process a batch", e);
      Thread.currentThread().interrupt();
    } catch (TimeoutException e) {
      LOG.error("Timed out while waiting to process a batch", e);
      future.cancel(true);
    } catch (ExecutionException | CancellationException e) {
      LOG.error("Error while processing a batch", e);
    }
  }

  @SneakyThrows
  void convert(ResultSet resultSet) {
    List<Row> rowList = new ArrayList<>();
    while (resultSet.next()) {
      Builder builder = Row.newBuilder();
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
                    : getNullValueEquivalent(metaData.getColumnType(c));
          }
          builder.addColumn(convertedColVal);
        }
      }
      rowList.add(builder.build());
    }
    synchronized (this) {
      rows.addAll(rowList);
    }
  }

  private Value getNullValueEquivalent(int columnType) {
    switch (columnType) {
      case Types.BIGINT:
      case Types.INTEGER:
      case Types.NUMERIC:
        return NULL_INTEGER_EQ_STRING_VALUE;
      case Types.FLOAT:
      case Types.DOUBLE:
        return NULL_FLOAT_EQ_STRING_VALUE;
      case Types.BOOLEAN:
        return NULL_BOOLEAN_EQ_STRING_VALUE;
      default:
        return NULL_STRING_EQ_STRING_VALUE;
    }
  }
}
