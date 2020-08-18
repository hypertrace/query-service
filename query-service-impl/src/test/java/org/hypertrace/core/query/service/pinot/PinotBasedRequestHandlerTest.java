package org.hypertrace.core.query.service.pinot;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.client.ResultSetGroup;
import org.hypertrace.core.query.service.QueryContext;
import org.hypertrace.core.query.service.QueryCost;
import org.hypertrace.core.query.service.QueryRequestBuilderUtils;
import org.hypertrace.core.query.service.QueryResultCollector;
import org.hypertrace.core.query.service.RequestAnalyzer;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.pinot.PinotClientFactory.PinotClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PinotBasedRequestHandlerTest {
  // Test subject
  private PinotBasedRequestHandler pinotBasedRequestHandler;
  private final ObjectMapper objectMapper = new ObjectMapper();

  @BeforeEach
  public void setUp() {
    // Mocks
    PinotClientFactory pinotClientFactoryMock = mock(PinotClientFactory.class);
    ResultSetTypePredicateProvider resultSetTypePredicateProviderMock = mock(
        ResultSetTypePredicateProvider.class);
    pinotBasedRequestHandler =
        new PinotBasedRequestHandler(resultSetTypePredicateProviderMock, pinotClientFactoryMock);

    // Test ResultTableResultSet result set format parsing
    when(resultSetTypePredicateProviderMock.isSelectionResultSetType(any(ResultSet.class)))
        .thenReturn(false);
    when(resultSetTypePredicateProviderMock.isResultTableResultSetType(any(ResultSet.class)))
        .thenReturn(true);
  }

  @Test
  public void testInit() {
    Config fileConfig = ConfigFactory.parseFile(new File(
        QueryRequestToPinotSQLConverterTest.class.getClassLoader()
            .getResource("application.conf").getFile()));
    Config serviceConfig = fileConfig.getConfig("service.config");
    for (Config config: serviceConfig.getConfigList("queryRequestHandlersConfig")) {
      PinotBasedRequestHandler handler = new PinotBasedRequestHandler();
      handler.init(config.getString("name"), config.getConfig("requestHandlerInfo"));
    }
  }

  @Test
  public void testInitFailure() {
    Assertions.assertThrows(RuntimeException.class, () -> {
      Config config = ConfigFactory.parseMap(Map.of("name", "test",
          "requestHandlerInfo", Map.of()));
      PinotBasedRequestHandler handler = new PinotBasedRequestHandler();
      handler.init(config.getString("name"), config.getConfig("requestHandlerInfo"));
    });
  }

  @Test
  public void testCanHandle() {
    Config fileConfig = ConfigFactory.parseFile(new File(
        QueryRequestToPinotSQLConverterTest.class.getClassLoader()
            .getResource("application.conf").getFile()));
    Config serviceConfig = fileConfig.getConfig("service.config");
    for (Config config: serviceConfig.getConfigList("queryRequestHandlersConfig")) {
      PinotBasedRequestHandler handler = new PinotBasedRequestHandler();
      handler.init(config.getString("name"), config.getConfig("requestHandlerInfo"));

      // Verify that the traces handler can traces query.
      if (config.getString("name").equals("trace-view-handler")) {
        QueryCost cost = handler.canHandle(
            QueryRequest.newBuilder()
                .setDistinctSelections(true)
                .addSelection(QueryRequestBuilderUtils.createColumnExpression("col1"))
                .addSelection(QueryRequestBuilderUtils.createColumnExpression("col2"))
                .addGroupBy(QueryRequestBuilderUtils.createColumnExpression("col3"))
                .build(),
            Set.of(), Set.of("Trace.start_time_millis", "Trace.end_time_millis", "Trace.duration_millis",
                "Trace.id", "Trace.tags"));
        Assertions.assertTrue(cost.getCost() >= 0.0d && cost.getCost() < 1.0d);
      }
    }
  }

  @Test
  public void testCanHandleNegativeCase() {
    Config fileConfig = ConfigFactory.parseFile(new File(
        QueryRequestToPinotSQLConverterTest.class.getClassLoader()
            .getResource("application.conf").getFile()));
    Config serviceConfig = fileConfig.getConfig("service.config");
    for (Config config: serviceConfig.getConfigList("queryRequestHandlersConfig")) {
      PinotBasedRequestHandler handler = new PinotBasedRequestHandler();
      handler.init(config.getString("name"), config.getConfig("requestHandlerInfo"));

      // Verify that the traces handler can traces query.
      if (config.getString("name").equals("trace-view-handler")) {
        QueryCost cost = handler.canHandle(
            QueryRequest.newBuilder()
                .setDistinctSelections(true)
                .addSelection(QueryRequestBuilderUtils.createColumnExpression("col1"))
                .addSelection(QueryRequestBuilderUtils.createColumnExpression("col2"))
                .addGroupBy(QueryRequestBuilderUtils.createColumnExpression("col3"))
                .build(),
            Set.of(), Set.of("Trace.does_not_exist", "Trace.end_time_millis",
                "Trace.duration_millis", "Trace.id", "Trace.tags"));
        Assertions.assertFalse(cost.getCost() >= 0.0d && cost.getCost() < 1.0d);
      }
    }
  }

  @Test
  public void testConvertSimpleSelectionsQueryResultSet() throws IOException {
    String[][] resultTable =
        new String[][] {
          {"operation-name-0", "service-name-0", "70", "80"},
          {"operation-name-1", "service-name-1", "71", "79"},
          {"operation-name-2", "service-name-2", "72", "78"},
          {"operation-name-3", "service-name-3", "73", "77"}
        };
    List<String> columnNames = List.of("operation_name", "service_name", "start_time_millis", "duration");
    ResultSet resultSet = mockResultSet(4, 4, columnNames, resultTable);
    ResultSetGroup resultSetGroup = mockResultSetGroup(List.of(resultSet));
    TestQueryResultCollector testQueryResultCollector = new TestQueryResultCollector();

    pinotBasedRequestHandler.convert(
        resultSetGroup, testQueryResultCollector, new LinkedHashSet<>());

    verifyResponseRows(testQueryResultCollector, resultTable);
  }

  @Test
  public void testConvertAggregationColumnsQueryResultSet() throws IOException {
    String[][] resultTable =
        new String[][] {
          {"operation-name-10", "110", "40", "21"},
          {"operation-name-11", "111", "41", "22"},
          {"operation-name-12", "112", "42", "23"},
          {"operation-name-13", "113", "43", "24"}
        };
    List<String> columnNames = List.of("operation_name", "avg(duration)", "count(*)", "max(duration)");
    ResultSet resultSet = mockResultSet(4, 4, columnNames, resultTable);
    ResultSetGroup resultSetGroup = mockResultSetGroup(List.of(resultSet));
    TestQueryResultCollector testQueryResultCollector = new TestQueryResultCollector();

    pinotBasedRequestHandler.convert(
        resultSetGroup, testQueryResultCollector, new LinkedHashSet<>());

    verifyResponseRows(testQueryResultCollector, resultTable);
  }

  @Test
  public void testConvertSelectionsWithMapKeysAndValuesQueryResultSet() throws IOException {
    String[][] resultTable =
        new String[][] {
          {
            "operation-name-11",
            stringify(List.of("t1", "t2")),
            stringify(List.of("v1", "v2")),
            "service-1",
            stringify(List.of("t10")),
            stringify(List.of("v10"))
          },
          {
            "operation-name-12",
            stringify(List.of("a2")),
            stringify(List.of("b2")),
            "service-2",
            stringify(List.of("c10", "c11")),
            stringify(List.of("d10", "d11"))
          },
          {
            "operation-name-13",
            stringify(List.of()),
            stringify(List.of()),
            "service-3",
            stringify(List.of("e15")),
            stringify(List.of("f15"))
          }
        };
    List<String> columnNames =
        List.of(
            "operation_name",
            "tags1" + ViewDefinition.MAP_KEYS_SUFFIX,
            "tags1" + ViewDefinition.MAP_VALUES_SUFFIX,
            "service_name",
            "tags2" + ViewDefinition.MAP_KEYS_SUFFIX,
            "tags2" + ViewDefinition.MAP_VALUES_SUFFIX);
    ResultSet resultSet = mockResultSet(3, 6, columnNames, resultTable);
    ResultSetGroup resultSetGroup = mockResultSetGroup(List.of(resultSet));
    TestQueryResultCollector testQueryResultCollector = new TestQueryResultCollector();

    pinotBasedRequestHandler.convert(
        resultSetGroup, testQueryResultCollector, new LinkedHashSet<>());

    String[][] expectedRows =
        new String[][] {
          {
            "operation-name-11",
            stringify(Map.of("t1", "v1", "t2", "v2")),
            "service-1",
            stringify(Map.of("t10", "v10"))
          },
          {
            "operation-name-12",
            stringify(Map.of("a2", "b2")),
            "service-2",
            stringify(Map.of("c10", "d10", "c11", "d11"))
          },
          {"operation-name-13", stringify(Map.of()), "service-3", stringify(Map.of("e15", "f15"))}
        };

    verifyResponseRows(testQueryResultCollector, expectedRows);
  }

  @Test
  public void testConvertMultipleResultSetsInFResultSetGroup() throws IOException {
    List<String> columnNames = List.of("operation_name", "avg(duration)", "count(*)", "max(duration)");
    String[][] resultTable1 =
        new String[][] {
          {"operation-name-10", "110", "40", "21"},
          {"operation-name-11", "111", "41", "22"},
          {"operation-name-12", "112", "42", "23"},
          {"operation-name-13", "113", "43", "24"}
        };
    ResultSet resultSet1 = mockResultSet(4, 4, columnNames, resultTable1);

    String[][] resultTable2 =
        new String[][] {
          {"operation-name-20", "200", "400", "20000"},
          {"operation-name-22", "220", "420", "22000"}
        };
    ResultSet resultSet2 = mockResultSet(2, 4, columnNames, resultTable2);
    ResultSetGroup resultSetGroup = mockResultSetGroup(List.of(resultSet1, resultSet2));
    TestQueryResultCollector testQueryResultCollector = new TestQueryResultCollector();

    pinotBasedRequestHandler.convert(
        resultSetGroup, testQueryResultCollector, new LinkedHashSet<>());

    String[][] expectedRows =
        new String[][] {
          {"operation-name-10", "110", "40", "21"},
          {"operation-name-11", "111", "41", "22"},
          {"operation-name-12", "112", "42", "23"},
          {"operation-name-13", "113", "43", "24"},
          {"operation-name-20", "200", "400", "20000"},
          {"operation-name-22", "220", "420", "22000"}
        };

    verifyResponseRows(testQueryResultCollector, expectedRows);
  }

  @Test
  public void testNullQueryRequestContextThrowsNPE() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> pinotBasedRequestHandler.handleRequest(
            null,
            QueryRequest.newBuilder().build(),
            mock(QueryResultCollector.class),
            mock(RequestAnalyzer.class)));
  }

  @Test
  public void testNullTenantIdQueryRequestContextThrowsNPE() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> pinotBasedRequestHandler.handleRequest(
            new QueryContext(null),
            QueryRequest.newBuilder().build(),
            mock(QueryResultCollector.class),
            mock(RequestAnalyzer.class)));
  }

  @Test
  public void
      testGroupBysAndAggregationsMixedWithSelectionsThrowsExceptionWhenDistinctSelectionIsSpecified() {
    // Setting distinct selections and mixing selections and group bys should throw exception
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> pinotBasedRequestHandler.handleRequest(
            new QueryContext("test-tenant-id"),
            QueryRequest.newBuilder()
                .setDistinctSelections(true)
                .addSelection(QueryRequestBuilderUtils.createColumnExpression("col1"))
                .addSelection(QueryRequestBuilderUtils.createColumnExpression("col2"))
                .addGroupBy(QueryRequestBuilderUtils.createColumnExpression("col3"))
                .build(),
            mock(QueryResultCollector.class),
            mock(RequestAnalyzer.class)));

    // Setting distinct selections and mixing selections and aggregations should throw exception
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> pinotBasedRequestHandler.handleRequest(
            new QueryContext("test-tenant-id"),
            QueryRequest.newBuilder()
                .setDistinctSelections(true)
                .addSelection(QueryRequestBuilderUtils.createColumnExpression("col1"))
                .addSelection(QueryRequestBuilderUtils.createColumnExpression("col2"))
                .addAggregation(
                    QueryRequestBuilderUtils.createFunctionExpression(
                        "AVG", "duration", "avg_duration"))
                .build(),
            mock(QueryResultCollector.class),
            mock(RequestAnalyzer.class)));

    // Setting distinct selections and mixing selections, group bys and aggregations should throw
    // exception
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> pinotBasedRequestHandler.handleRequest(
            new QueryContext("test-tenant-id"),
            QueryRequest.newBuilder()
                .setDistinctSelections(true)
                .addSelection(QueryRequestBuilderUtils.createColumnExpression("col1"))
                .addSelection(QueryRequestBuilderUtils.createColumnExpression("col2"))
                .addGroupBy(QueryRequestBuilderUtils.createColumnExpression("col3"))
                .addAggregation(
                    QueryRequestBuilderUtils.createFunctionExpression(
                        "AVG", "duration", "avg_duration"))
                .build(),
            mock(QueryResultCollector.class),
            mock(RequestAnalyzer.class)));
  }

  @Test
  public void testWithMockPinotClient() throws Exception {
    Config fileConfig = ConfigFactory.parseFile(new File(
        QueryRequestToPinotSQLConverterTest.class.getClassLoader()
            .getResource("application.conf").getFile()));
    Config serviceConfig = fileConfig.getConfig("service.config");
    for (Config config : serviceConfig.getConfigList("queryRequestHandlersConfig")) {
      if (!config.getString("name").equals("trace-view-handler")) {
        continue;
      }

      // Mock the PinotClient
      PinotClient pinotClient = mock(PinotClient.class);
      PinotClientFactory factory = mock(PinotClientFactory.class);
      when(factory.getPinotClient(any())).thenReturn(pinotClient);

      String[][] resultTable =
          new String[][] {
              {"operation-name-0", "service-name-0", "70", "80"},
              {"operation-name-1", "service-name-1", "71", "79"},
              {"operation-name-2", "service-name-2", "72", "78"},
              {"operation-name-3", "service-name-3", "73", "77"}
          };
      List<String> columnNames = List.of("operation_name", "service_name", "start_time_millis", "duration");
      ResultSet resultSet = mockResultSet(4, 4, columnNames, resultTable);
      ResultSetGroup resultSetGroup = mockResultSetGroup(List.of(resultSet));
      when(pinotClient.executeQuery(any(), any())).thenReturn(resultSetGroup);

      PinotBasedRequestHandler handler =
          new PinotBasedRequestHandler(new ResultSetTypePredicateProvider() {
            @Override
            public boolean isSelectionResultSetType(ResultSet resultSet) {
              return true;
            }

            @Override
            public boolean isResultTableResultSetType(ResultSet resultSet) {
              return false;
            }
          }, factory);
      handler.init(config.getString("name"), config.getConfig("requestHandlerInfo"));

      TestQueryResultCollector testQueryResultCollector = new TestQueryResultCollector();

      QueryContext context = new QueryContext("__default");
      handler.handleRequest(context,
          QueryRequest.newBuilder()
              .addSelection(QueryRequestBuilderUtils.createColumnExpression("col1"))
              .addSelection(QueryRequestBuilderUtils.createColumnExpression("col2"))
              .build(),
          testQueryResultCollector, mock(RequestAnalyzer.class));
      Assertions.assertNotNull(testQueryResultCollector.getResultSetChunk());
    }
  }

  private ResultSet mockResultSet(
      int rowCount, int columnCount, List<String> columnNames, String[][] resultsTable) {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getRowCount()).thenReturn(rowCount);
    when(resultSet.getColumnCount()).thenReturn(columnCount);
    for (int colIdx = 0; colIdx < columnNames.size(); colIdx++) {
      when(resultSet.getColumnName(colIdx)).thenReturn(columnNames.get(colIdx));
    }

    for (int rowIdx = 0; rowIdx < resultsTable.length; rowIdx++) {
      for (int colIdx = 0; colIdx < resultsTable[0].length; colIdx++) {
        when(resultSet.getString(rowIdx, colIdx)).thenReturn(resultsTable[rowIdx][colIdx]);
      }
    }

    return resultSet;
  }

  private ResultSetGroup mockResultSetGroup(List<ResultSet> resultSets) {
    ResultSetGroup resultSetGroup = mock(ResultSetGroup.class);

    when(resultSetGroup.getResultSetCount()).thenReturn(resultSets.size());
    for (int i = 0; i < resultSets.size(); i++) {
      when(resultSetGroup.getResultSet(i)).thenReturn(resultSets.get(i));
    }

    return resultSetGroup;
  }

  private void verifyResponseRows(
      TestQueryResultCollector testQueryResultCollector, String[][] expectedResultTable)
      throws IOException {
    List<Row> rows = testQueryResultCollector.getResultSetChunk().getRowList();
    Assertions.assertEquals(expectedResultTable.length, rows.size());
    for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
      Row row = rows.get(rowIdx);
      Assertions.assertEquals(expectedResultTable[rowIdx].length, row.getColumnCount());
      for (int colIdx = 0; colIdx < row.getColumnCount(); colIdx++) {
        String val = row.getColumn(colIdx).getString();
        // In the scope of our unit tests, this is a map. Cannot JSON object comparison on it since
        // it's not ordered.
        if (val.startsWith("{") && val.endsWith("}")) {
          Assertions.assertEquals(
              objectMapper.readTree(expectedResultTable[rowIdx][colIdx]),
              objectMapper.readTree(val));
        } else {
          Assertions.assertEquals(expectedResultTable[rowIdx][colIdx], val);
        }
      }
    }
  }

  private String stringify(Object obj) throws JsonProcessingException {
    return objectMapper.writeValueAsString(obj);
  }

  static class TestQueryResultCollector implements QueryResultCollector<Row> {
    private final ResultSetChunk.Builder resultSetChunkBuilder = ResultSetChunk.newBuilder();

    @Override
    public void collect(Row row) {
      resultSetChunkBuilder.addRow(row);
    }

    @Override
    public void finish() {}

    public ResultSetChunk getResultSetChunk() {
      return resultSetChunkBuilder.build();
    }
  }
}
