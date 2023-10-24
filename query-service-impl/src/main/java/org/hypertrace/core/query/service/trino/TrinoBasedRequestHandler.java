package org.hypertrace.core.query.service.trino;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
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
import java.util.Map.Entry;
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
import org.hypertrace.core.query.service.trino.converters.TrinoFunctionConverter;
import org.hypertrace.core.query.service.trino.converters.TrinoFunctionConverterConfig;
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
  private TableDefinition tableDefinition;
  private Optional<String> startTimeAttributeName;
  private QueryRequestToTrinoSQLConverter request2TrinoSqlConverter;
  private final TrinoClientFactory trinoClientFactory;
  private final TrinoFilterHandler trinoFilterHandler;

  private final JsonFormat.Printer protoJsonPrinter =
      JsonFormat.printer().omittingInsignificantWhitespace();

  TrinoBasedRequestHandler(String name, Config config) {
    this(name, config, TrinoClientFactory.get());
  }

  TrinoBasedRequestHandler(String name, Config config, TrinoClientFactory trinoClientFactory) {
    this.name = name;
    this.trinoClientFactory = trinoClientFactory;
    this.processConfig(config);
    this.trinoFilterHandler = new TrinoFilterHandler();
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
  public QueryCost canHandle(QueryRequest request, ExecutionContext executionContext) {
    Set<String> referencedColumns = executionContext.getReferencedColumns();

    Preconditions.checkArgument(!referencedColumns.isEmpty());

    // query must contain isTrino attribute filter
    if (!trinoFilterHandler.containsAttributeFilter(request)) {
      return QueryCost.UNSUPPORTED;
    }

    for (String referencedColumn : referencedColumns) {
      if (trinoFilterHandler.matchesIsTrinoAttribute(referencedColumn)) {
        continue;
      }
      if (!tableDefinition.containsColumn(referencedColumn)) {
        return QueryCost.UNSUPPORTED;
      }
    }

    // TODO: add logic
    return new QueryCost(0);
  }

  @Override
  public Observable<Row> handleRequest(QueryRequest request, ExecutionContext executionContext) {
    try {
      Entry<String, Params> sql =
          request2TrinoSqlConverter.toSQL(
              executionContext, request, executionContext.getAllSelections());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Trying to execute SQL: [ {} ] by RequestHandler: [ {} ]", sql, this.getName());
      }

      return executeQuery(sql.getKey(), sql.getValue());
    } catch (Throwable t) {
      return Observable.error(t);
    }
  }

  private void processConfig(Config config) {

    if (!config.hasPath(TENANT_COLUMN_NAME_CONFIG_KEY)) {
      throw new RuntimeException(
          TENANT_COLUMN_NAME_CONFIG_KEY + " is not defined in the " + name + " request handler.");
    }
    String tenantColumnName = config.getString(TENANT_COLUMN_NAME_CONFIG_KEY);

    Optional<String> countColumnName =
        config.hasPath(COUNT_COLUMN_NAME_CONFIG_KEY)
            ? Optional.of(config.getString(COUNT_COLUMN_NAME_CONFIG_KEY))
            : Optional.empty();

    this.startTimeAttributeName =
        config.hasPath(START_TIME_ATTRIBUTE_NAME_CONFIG_KEY)
            ? Optional.of(config.getString(START_TIME_ATTRIBUTE_NAME_CONFIG_KEY))
            : Optional.empty();

    this.tableDefinition =
        TableDefinition.parse(
            config.getConfig(TABLE_DEFINITION_CONFIG_KEY), tenantColumnName, countColumnName);

    this.request2TrinoSqlConverter =
        new QueryRequestToTrinoSQLConverter(
            tableDefinition,
            new TrinoFunctionConverter(tableDefinition, new TrinoFunctionConverterConfig(config)));
  }

  public Observable<Row> executeQuery(String statement, Params params) throws SQLException {
    final TrinoClient trinoClient = trinoClientFactory.getTrinoClient(this.getName());
    String resolvedStatement = request2TrinoSqlConverter.resolveStatement(statement, params);
    Connection connection = trinoClient.getConnection();
    try (PreparedStatement preparedStatement = connection.prepareStatement(resolvedStatement);
        ResultSet resultSet = preparedStatement.executeQuery()) {
      LOG.debug("Query results: [ {} ]", resultSet);
      return convert(resultSet);
    } catch (Exception ex) {
      // Catch this exception to log the Trino SQL query that caused the issue
      LOG.error("An error occurred while executing: {}", resolvedStatement, ex);
      // Rethrow for the caller to return an error.
      throw new RuntimeException(ex);
    }
  }

  @SneakyThrows
  Observable<Row> convert(ResultSet resultSet) {
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
