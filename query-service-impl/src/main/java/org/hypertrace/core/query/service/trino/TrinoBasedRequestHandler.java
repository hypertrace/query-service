package org.hypertrace.core.query.service.trino;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import io.reactivex.rxjava3.core.Observable;
import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.SneakyThrows;
import org.hypertrace.core.query.service.ExecutionContext;
import org.hypertrace.core.query.service.QueryCost;
import org.hypertrace.core.query.service.RequestHandler;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.Row.Builder;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;
import org.hypertrace.core.query.service.trino.TrinoClientFactory.TrinoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrinoBasedRequestHandler implements RequestHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TrinoBasedRequestHandler.class);

  public static final String TABLE_DEFINITION_CONFIG_KEY = "tableDefinition";
  private static final String TENANT_COLUMN_NAME_CONFIG_KEY = "tenantColumnName";
  private static final String COUNT_COLUMN_NAME_CONFIG_KEY = "countColumnName";
  private static final String START_TIME_ATTRIBUTE_NAME_CONFIG_KEY = "startTimeAttributeName";
  private static final String SLOW_QUERY_THRESHOLD_MS_CONFIG = "slowQueryThresholdMs";
  private static final String MIN_REQUEST_DURATION_KEY = "minRequestDuration";

  private static final int DEFAULT_SLOW_QUERY_THRESHOLD_MS = 3000;
  private static final Set<Operator> GTE_OPERATORS = Set.of(Operator.GE, Operator.GT, Operator.EQ);
  private static final Set<Operator> LTE_OPERATORS = Set.of(Operator.LE, Operator.LT);

  // string values equivalent for null value of different data types
  // this is required to keep null values equivalent to default values for
  // various data types in pinot implementation
  private static final Value NULL_STRING_EQ_STRING_VALUE =
      Value.newBuilder().setValueType(ValueType.STRING).setString("null").build();
  private static final Value NULL_INTEGER_EQ_STRING_VALUE =
      Value.newBuilder().setValueType(ValueType.STRING).setString("0").build();
  private static final Value NULL_FLOAT_EQ_STRING_VALUE =
      Value.newBuilder().setValueType(ValueType.STRING).setString("0.0").build();
  private static final Value NULL_BOOLEAN_EQ_STRING_VALUE =
      Value.newBuilder().setValueType(ValueType.STRING).setString("false").build();

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String name;
  private Optional<String> startTimeAttributeName;
  private final TrinoClientFactory trinoClientFactory;

  TrinoBasedRequestHandler(String name, Config config) {
    this(name, config, TrinoClientFactory.get());
  }

  TrinoBasedRequestHandler(String name, Config config, TrinoClientFactory trinoClientFactory) {
    this.name = name;
    this.trinoClientFactory = trinoClientFactory;
    this.processConfig(config);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Optional<String> getTimeFilterColumn() {
    return this.startTimeAttributeName;
  }

  @Override
  public QueryCost canHandle(QueryRequest request, ExecutionContext context) {
    // Only interactive queries are supported
    if (!request.getInteractive()) {
      return QueryCost.UNSUPPORTED;
    }
    // TODO: add logic
    return new QueryCost(0);
  }

  @Override
  public Observable<Row> handleRequest(QueryRequest request, ExecutionContext executionContext) {
    try {
      return executeQuery();
    } catch (Throwable t) {
      return Observable.error(t);
    }
  }

  private void processConfig(Config config) {

    if (!config.hasPath(TENANT_COLUMN_NAME_CONFIG_KEY)) {
      throw new RuntimeException(
          TENANT_COLUMN_NAME_CONFIG_KEY + " is not defined in the " + name + " request handler.");
    }

    this.startTimeAttributeName =
        config.hasPath(START_TIME_ATTRIBUTE_NAME_CONFIG_KEY)
            ? Optional.of(config.getString(START_TIME_ATTRIBUTE_NAME_CONFIG_KEY))
            : Optional.empty();

    // TODO
  }

  private Observable<Row> executeQuery() throws SQLException {
    final TrinoClient trinoClient = trinoClientFactory.getTrinoClient(this.getName());
    String sql =
        "Select api_id, api_name, service_id, service_name, COUNT(*) as count "
            + "from span_event_view_staging_test "
            + "where customer_id = 'b227d0f9-98e1-4eff-acf5-ab129d416914' "
            + "and start_time_millis >= 1692943200000  and start_time_millis < 1692946800000 "
            + "AND api_id != 'null' AND api_discovery_state IN ('DISCOVERED', 'UNDER_DISCOVERY') "
            + "GROUP BY api_id, api_name, service_name, service_id limit 20";
    Connection connection = trinoClient.getConnection();
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      LOG.debug("Query results: [ {} ]", resultSet);
      return convert(resultSet);
    } catch (Exception ex) {
      // Catch this exception to log the Postgres SQL query that caused the issue
      LOG.error("An error occurred while executing: {}", sql, ex);
      // Rethrow for the caller to return an error.
      throw new RuntimeException(ex);
    }
  }

  @SneakyThrows
  Observable<Row> convert(ResultSet resultSet) {
    String api_id, api_name, service_name, service_id = null;
    int count = 0;
    int total = 0;
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

      api_id = resultSet.getString("api_id");
      api_name = resultSet.getString("api_name");
      service_id = resultSet.getString("service_id");
      service_name = resultSet.getString("service_name");
      count = resultSet.getInt("count");
      System.out.println(
          String.format("%s, %s, %s, %s, %d", api_id, api_name, service_id, service_name, count));
      total++;
    }
    System.out.println("total: " + total);
    return Observable.fromIterable(rowList).doOnNext(row -> LOG.debug("collect a row: {}", row));
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
