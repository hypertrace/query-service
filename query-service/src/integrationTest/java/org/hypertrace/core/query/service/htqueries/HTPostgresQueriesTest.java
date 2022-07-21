package org.hypertrace.core.query.service.htqueries;

import static com.github.stefanbirkner.systemlambda.SystemLambda.withEnvironmentVariable;
import static org.hypertrace.core.query.service.QueryServiceTestUtils.getAttributeExpressionQuery;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.google.protobuf.InvalidProtocolBufferException;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.compress.utils.IOUtils;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.core.query.service.client.QueryServiceConfig;
import org.hypertrace.core.serviceframework.IntegrationTestServerUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class HTPostgresQueriesTest {

  private static final Logger LOG = LoggerFactory.getLogger(HTPostgresQueriesTest.class);
  private static final Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(LOG);
  private static final Map<String, String> TENANT_ID_MAP = Map.of("x-tenant-id", "__default");
  private static final int CONTAINER_STARTUP_ATTEMPTS = 5;

  private static Network network;
  private static GenericContainer<?> mongo;
  private static GenericContainer<?> attributeService;
  private static GenericContainer<?> postgresqlService;

  private static QueryServiceClient queryServiceClient;

  @BeforeAll
  public static void setup() throws Exception {
    network = Network.newNetwork();

    mongo =
        new GenericContainer<>(DockerImageName.parse("hypertrace/mongodb:main"))
            .withNetwork(network)
            .withNetworkAliases("mongo")
            .withExposedPorts(27017)
            .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
            .waitingFor(Wait.forLogMessage(".*waiting for connections on port 27017.*", 1));
    mongo.start();
    mongo.followOutput(logConsumer);

    attributeService =
        new GenericContainer<>(DockerImageName.parse("hypertrace/attribute-service:main"))
            .withNetwork(network)
            .withNetworkAliases("attribute-service")
            .withEnv("MONGO_HOST", "mongo")
            .withExposedPorts(9012)
            .dependsOn(mongo)
            .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
            .waitingFor(Wait.forLogMessage(".*Started admin service on port: 9013.*", 1));
    attributeService.start();
    attributeService.followOutput(logConsumer);

    postgresqlService =
        new GenericContainer<>(DockerImageName.parse("bitnami/postgresql:latest"))
            .withNetwork(network)
            .withNetworkAliases("postgresql")
            .withEnv(
                Map.of(
                    "POSTGRES_USER",
                    "postgres",
                    "POSTGRES_PASSWORD",
                    "postgres",
                    "PGPASSWORD",
                    "postgres"))
            .withExposedPorts(5432)
            .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
            .waitingFor(
                Wait.forLogMessage(".*database system is ready to accept connections.*", 1));
    postgresqlService.start();
    postgresqlService.followOutput(logConsumer);

    runSqlInPostgresDb(
        "sql/functions.sql",
        "sql/raw-service-view-events.sql",
        "sql/raw-service-view-events-insert.sql");

    withEnvironmentVariable(
            "POSTGRES_CONNECT_STR",
            "jdbc:postgresql://localhost:"
                + postgresqlService.getMappedPort(5432).toString()
                + "/postgres?user=postgres&password=postgres")
        .and("ATTRIBUTE_SERVICE_HOST_CONFIG", attributeService.getHost())
        .and("ATTRIBUTE_SERVICE_PORT_CONFIG", attributeService.getMappedPort(9012).toString())
        .execute(() -> IntegrationTestServerUtil.startServices(new String[] {"query-service"}));

    Map<String, Object> map = Maps.newHashMap();
    map.put("host", "localhost");
    map.put("port", 8090);
    QueryServiceConfig queryServiceConfig = new QueryServiceConfig(ConfigFactory.parseMap(map));
    queryServiceClient = new QueryServiceClient(queryServiceConfig);
  }

  @AfterAll
  public static void shutdown() {
    LOG.info("Initiating shutdown");
    attributeService.stop();
    mongo.stop();
    postgresqlService.stop();
    network.close();
  }

  private static void runSqlInPostgresDb(String... sqlFileNames)
      throws IOException, InterruptedException {
    int count = 0;
    long currentTimeMillis = System.currentTimeMillis();
    for (String sqlFileName : sqlFileNames) {
      count++;
      byte[] resourceBytes;
      try (InputStream resourceAsStream =
          HTPostgresQueriesTest.class.getClassLoader().getResourceAsStream(sqlFileName)) {
        resourceBytes =
            StringUtils.getBytesUtf8(
                StringUtils.newStringUtf8(IOUtils.toByteArray(resourceAsStream))
                    .replaceAll("START_TIME_MIILIS", String.valueOf(currentTimeMillis)));
      }
      if (resourceBytes != null) {
        postgresqlService.copyFileToContainer(
            Transferable.of(resourceBytes), "/tmp/" + count + ".sql");
        Container.ExecResult execResult =
            postgresqlService.execInContainer(
                "/opt/bitnami/postgresql/bin/psql",
                "-U",
                "postgres",
                "-f",
                "/tmp/" + count + ".sql",
                "-q",
                "-d",
                "postgres");
        LOG.info("sql command output : {}", execResult);
      }
    }
  }

  private static void validateRows(List<Row> rows, double divisor) {
    rows.forEach(
        row -> {
          double val1 = Double.parseDouble(row.getColumn(2).getString());
          double val2 = Double.parseDouble(row.getColumn(3).getString()) / divisor;
          assertTrue(Math.abs(val1 - val2) < Math.pow(10, -3));
        });
  }

  @ParameterizedTest
  @MethodSource("provideQueryRequestForServiceQueries")
  public void testServicesQueries(QueryRequest queryRequest) {
    LOG.info("Services queries");
    Iterator<ResultSetChunk> itr =
        queryServiceClient.executeQuery(queryRequest, TENANT_ID_MAP, 10000);
    List<ResultSetChunk> list = Streams.stream(itr).collect(Collectors.toList());
    List<Row> rows = list.get(0).getRowList();
    assertEquals(4, rows.size());
    List<String> serviceNames =
        new ArrayList<>(Arrays.asList("frontend", "driver", "route", "customer"));
    rows.forEach(row -> serviceNames.remove(row.getColumn(1).getString()));
    assertTrue(serviceNames.isEmpty());
  }

  @Test
  public void testServicesQueriesForAvgRate() {
    LOG.info("Services queries for AVGRATE");
    Iterator<ResultSetChunk> itr =
        queryServiceClient.executeQuery(ServicesQueries.buildAvgRateQuery(), TENANT_ID_MAP, 10000);
    List<ResultSetChunk> list = Streams.stream(itr).collect(Collectors.toList());
    List<Row> rows = list.get(0).getRowList();
    assertEquals(4, rows.size());
    List<String> serviceNames =
        new ArrayList<>(Arrays.asList("frontend", "driver", "route", "customer"));
    rows.forEach(row -> serviceNames.remove(row.getColumn(1).getString()));
    assertTrue(serviceNames.isEmpty());
    validateRows(rows, 3600);
  }

  @Test
  public void testServicesQueriesWithAvgRateinOrderBy() {
    LOG.info("Services queries for AVGRATE in Order By");
    Iterator<ResultSetChunk> itr =
        queryServiceClient.executeQuery(
            ServicesQueries.buildAvgRateQueryForOrderBy(), TENANT_ID_MAP, 10000);
    List<ResultSetChunk> list = Streams.stream(itr).collect(Collectors.toList());
    List<Row> rows = list.get(0).getRowList();
    assertEquals(4, rows.size());
    List<String> serviceNames =
        new ArrayList<>(Arrays.asList("frontend", "driver", "route", "customer"));
    rows.forEach(row -> serviceNames.remove(row.getColumn(1).getString()));
    assertTrue(serviceNames.isEmpty());
  }

  @Test
  public void testServicesQueriesForAvgRateWithTimeAggregation() {
    LOG.info("Services queries for AVGRATE with time aggregation");
    Iterator<ResultSetChunk> itr =
        queryServiceClient.executeQuery(
            ServicesQueries.buildAvgRateQueryWithTimeAggregation(), TENANT_ID_MAP, 10000);
    List<ResultSetChunk> list = Streams.stream(itr).collect(Collectors.toList());
    List<Row> rows = list.get(0).getRowList();
    assertEquals(4, rows.size());
    validateRows(rows, 15);
  }

  private static Stream<Arguments> provideQueryRequestForServiceQueries()
      throws InvalidProtocolBufferException {
    QueryRequest queryRequest1 = ServicesQueries.buildQuery1();
    return Stream.of(
        Arguments.arguments(queryRequest1),
        Arguments.arguments(getAttributeExpressionQuery(queryRequest1)));
  }
}
