package org.hypertrace.core.query.service.htqueries;

import static com.github.stefanbirkner.systemlambda.SystemLambda.withEnvironmentVariable;
import static org.hypertrace.core.query.service.QueryServiceTestUtils.buildQueryFromJsonFile;
import static org.hypertrace.core.query.service.QueryServiceTestUtils.getAttributeExpressionQuery;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.google.protobuf.InvalidProtocolBufferException;
import com.typesafe.config.ConfigFactory;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.hypertrace.core.attribute.service.client.AttributeServiceClient;
import org.hypertrace.core.attribute.service.v1.AttributeMetadataFilter;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class HTPinotQueriesTest {

  private static final Logger LOG = LoggerFactory.getLogger(HTPinotQueriesTest.class);
  private static final Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(LOG);
  private static final Map<String, String> TENANT_ID_MAP = Map.of("x-tenant-id", "__default");
  private static final int CONTAINER_STARTUP_ATTEMPTS = 5;

  private static AdminClient adminClient;
  private static String bootstrapServers;

  private static Network network;
  private static GenericContainer<?> mongo;
  private static GenericContainer<?> attributeService;
  private static KafkaContainer kafkaZk;
  private static GenericContainer<?> pinotServiceManager;

  private static QueryServiceClient queryServiceClient;

  @BeforeAll
  public static void setup() throws Exception {
    network = Network.newNetwork();

    kafkaZk =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.0"))
            .withNetwork(network)
            .withNetworkAliases("kafka", "zookeeper")
            .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
            .waitingFor(Wait.forListeningPort());
    kafkaZk.start();

    pinotServiceManager =
        new GenericContainer<>(DockerImageName.parse("traceableai-docker.jfrog.io/hypertrace/pinot-servicemanager:test"))
            .withNetwork(network)
            .withNetworkAliases("pinot-controller", "pinot-server", "pinot-broker")
            .withExposedPorts(8099, 9000)
            .dependsOn(kafkaZk)
            .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
            .waitingFor(Wait.forLogMessage(".*Completed schema installation.*", 1))
            .withLogConsumer(logConsumer);
    pinotServiceManager.start();

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

    List<String> topicsNames =
        List.of(
            "enriched-structured-traces",
            "raw-service-view-events",
            "raw-trace-view-events",
            "service-call-view-events",
            "span-event-view",
            "backend-entity-view-events",
            "log-event-view",
            "raw-logs");
    bootstrapServers = kafkaZk.getBootstrapServers();
    adminClient =
        AdminClient.create(
            Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaZk.getBootstrapServers()));
    List<NewTopic> topics =
        topicsNames.stream().map(v -> new NewTopic(v, 1, (short) 1)).collect(Collectors.toList());
    adminClient.createTopics(topics);

    assertTrue(bootstrapConfig());
    LOG.info("Bootstrap Complete");
    assertTrue(generateData());
    LOG.info("Generate Data Complete");

    withEnvironmentVariable("PINOT_CONNECTION_TYPE", "broker")
        .and("ZK_CONNECT_STR", "localhost:" + pinotServiceManager.getMappedPort(8099).toString())
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
    pinotServiceManager.stop();
    kafkaZk.stop();
    network.close();
  }

  private static boolean bootstrapConfig() throws Exception {
    GenericContainer<?> bootstrapper =
        new GenericContainer<>(DockerImageName.parse("hypertrace/config-bootstrapper:main"))
            .withNetwork(network)
            .dependsOn(attributeService)
            .withEnv("MONGO_HOST", "mongo")
            .withEnv("ATTRIBUTE_SERVICE_HOST_CONFIG", "attribute-service")
            .withCommand(
                "-c",
                "/app/resources/configs/config-bootstrapper/application.conf",
                "-C",
                "/app/resources/configs/config-bootstrapper/attribute-service",
                "--upgrade")
            .withLogConsumer(logConsumer);
    bootstrapper.start();

    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(
                attributeService.getHost(), attributeService.getMappedPort(9012))
            .usePlaintext()
            .build();
    AttributeServiceClient client = new AttributeServiceClient(channel);
    int retry = 0;
    while (Streams.stream(
                    client.findAttributes(
                        TENANT_ID_MAP, AttributeMetadataFilter.getDefaultInstance()))
                .collect(Collectors.toList())
                .size()
            == 0
        && retry++ < 5) {
      Thread.sleep(2000);
    }
    channel.shutdown();
    bootstrapper.stop();
    return retry < 5;
  }

  private static boolean generateData() throws Exception {
    // start view-gen service
    GenericContainer<?> viewGen =
        new GenericContainer(DockerImageName.parse("hypertrace/hypertrace-view-generator:main"))
            .withNetwork(network)
            .dependsOn(kafkaZk)
            .withEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
            .withEnv(
                "DEFAULT_KEY_SERDE", "org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde")
            .withEnv(
                "DEFAULT_VALUE_SERDE",
                "org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde")
            .withEnv("NUM_STREAM_THREADS", "1")
            .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
            .waitingFor(Wait.forLogMessage(".* Started admin service on port: 8099.*", 1));
    viewGen.start();
    viewGen.followOutput(logConsumer);

    // produce data
    SpecificDatumReader<StructuredTrace> datumReader =
        new SpecificDatumReader<>(StructuredTrace.getClassSchema());

    DataFileReader<StructuredTrace> dfrStructuredTrace =
        new DataFileReader<>(
            new File(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("StructuredTrace-Hotrod.avro")
                    .getPath()),
            datumReader);

    StructuredTrace trace = dfrStructuredTrace.next();
    dfrStructuredTrace.close();

    updateTraceTimeStamp(trace);
    KafkaProducer<String, StructuredTrace> producer =
        new KafkaProducer<>(
            ImmutableMap.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers,
                ProducerConfig.CLIENT_ID_CONFIG,
                UUID.randomUUID().toString()),
            new StringSerializer(),
            new AvroSerde<StructuredTrace>().serializer());
    producer.send(new ProducerRecord<>("enriched-structured-traces", "", trace)).get();

    Map<String, Long> endOffSetMap =
        Map.of(
            "raw-service-view-events", 13L,
            "backend-entity-view-events", 11L,
            "raw-trace-view-events", 1L,
            "service-call-view-events", 27L,
            "span-event-view", 50L,
            "log-event-view", 0L);
    int retry = 0, maxRetries = 50;
    while (!areMessagesConsumed(endOffSetMap) && retry++ < maxRetries) {
      Thread.sleep(6000); // max 5 min wait time
    }
    // stop this service
    viewGen.stop();

    return retry < maxRetries;
  }

  private static boolean areMessagesConsumed(Map<String, Long> endOffSetMap) throws Exception {
    ListConsumerGroupsResult listConsumerGroups = adminClient.listConsumerGroups();
    List<String> groupIds = listConsumerGroups.all().get().stream()
        .filter(consumerGroupListing -> consumerGroupListing.isSimpleConsumerGroup())
        .map(consumerGroupListing -> consumerGroupListing.groupId())
        .collect(Collectors.toUnmodifiableList());

    Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>();
    for(String groupId : groupIds) {
      ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = adminClient.listConsumerGroupOffsets(groupId);
      Map<TopicPartition, OffsetAndMetadata> metadataMap = listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get();
      metadataMap.forEach((k, v) -> offsetAndMetadataMap.putIfAbsent(k, v));
    }

    if (offsetAndMetadataMap.size() < 6) {
      return false;
    }
    return offsetAndMetadataMap.entrySet().stream()
        .noneMatch(k -> k.getValue().offset() < endOffSetMap.get(k.getKey().topic()));
  }

  private static void updateTraceTimeStamp(StructuredTrace trace) {
    long delta = System.currentTimeMillis() - trace.getStartTimeMillis();
    trace.setStartTimeMillis(trace.getStartTimeMillis() + delta);
    trace.setEndTimeMillis(trace.getEndTimeMillis() + delta);
    // update events
    trace.getEventList().forEach(e -> e.setStartTimeMillis(e.getStartTimeMillis() + delta));
    trace.getEventList().forEach(e -> e.setEndTimeMillis(e.getEndTimeMillis() + delta));
    // updates edges
    trace
        .getEntityEdgeList()
        .forEach(edge -> edge.setStartTimeMillis(edge.getStartTimeMillis() + delta));
    trace
        .getEntityEdgeList()
        .forEach(edge -> edge.setEndTimeMillis(edge.getEndTimeMillis() + delta));
    trace
        .getEventEdgeList()
        .forEach(edge -> edge.setStartTimeMillis(edge.getStartTimeMillis() + delta));
    trace
        .getEventEdgeList()
        .forEach(edge -> edge.setEndTimeMillis(edge.getEndTimeMillis() + delta));
    trace
        .getEntityEventEdgeList()
        .forEach(edge -> edge.setStartTimeMillis(edge.getStartTimeMillis() + delta));
    trace
        .getEntityEventEdgeList()
        .forEach(edge -> edge.setEndTimeMillis(edge.getEndTimeMillis() + delta));
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

  @ParameterizedTest
  @MethodSource("provideQueryRequestForBackendQueries")
  public void testBackendsQueries(QueryRequest queryRequest) {
    LOG.info("Backends queries");
    Iterator<ResultSetChunk> itr =
        queryServiceClient.executeQuery(queryRequest, TENANT_ID_MAP, 10000);
    List<ResultSetChunk> list = Streams.stream(itr).collect(Collectors.toList());
    List<Row> rows = list.get(0).getRowList();
    assertEquals(1, rows.size());
    List<String> backendNames = new ArrayList<>(Collections.singletonList("redis"));
    rows.forEach(row -> backendNames.remove(row.getColumn(1).getString()));
    assertTrue(backendNames.isEmpty());
  }

  @ParameterizedTest
  @MethodSource("provideQueryRequestForExplorerQueries")
  public void testExplorerQueries(QueryRequest queryRequest) {
    LOG.info("Explorer queries");
    Iterator<ResultSetChunk> itr =
        queryServiceClient.executeQuery(queryRequest, TENANT_ID_MAP, 10000);
    List<ResultSetChunk> list = Streams.stream(itr).collect(Collectors.toList());
    List<Row> rows = list.get(0).getRowList();
    assertEquals(1, rows.size());
    // COUNT_API_TRACE.calls_[] is 13
    assertEquals("13", rows.get(0).getColumn(1).getString());
  }

  @ParameterizedTest
  @MethodSource("provideQueryRequestForAttributeExpressionQueries")
  public void testAttributeExpressionQueries(
      QueryRequest queryRequest, int rowSize, String expectedValue) {
    LOG.info("Attribute Expression queries");
    Iterator<ResultSetChunk> itr =
        queryServiceClient.executeQuery(queryRequest, TENANT_ID_MAP, 10000);
    List<ResultSetChunk> list = Streams.stream(itr).collect(Collectors.toList());
    List<Row> rows = list.get(0).getRowList();
    assertEquals(rowSize, rows.size());
    assertEquals(expectedValue, rows.get(0).getColumn(0).getString());
  }

  @Test
  public void testNotContainsKeyQuery() throws IOException {
    LOG.info("Not contains key query");
    Iterator<ResultSetChunk> itr =
        queryServiceClient.executeQuery(
            buildQueryFromJsonFile("not_contains_key_query.json"), TENANT_ID_MAP, 10000);
    List<ResultSetChunk> list = Streams.stream(itr).collect(Collectors.toList());
    List<Row> rows = list.get(0).getRowList();
    assertEquals(10, rows.size());
    assertEquals(
        "\"http.status_code\":\"200\"", rows.get(0).getColumn(0).getString().split(",")[1]);
  }

  private static Stream<Arguments> provideQueryRequestForServiceQueries()
      throws InvalidProtocolBufferException {
    QueryRequest queryRequest1 = ServicesQueries.buildQuery1();
    return Stream.of(
        Arguments.arguments(queryRequest1),
        Arguments.arguments(getAttributeExpressionQuery(queryRequest1)));
  }

  private static Stream<Arguments> provideQueryRequestForBackendQueries()
      throws InvalidProtocolBufferException {
    QueryRequest queryRequest1 = BackendsQueries.buildQuery1();
    return Stream.of(
        Arguments.arguments(queryRequest1),
        Arguments.arguments(getAttributeExpressionQuery(queryRequest1)));
  }

  private static Stream<Arguments> provideQueryRequestForExplorerQueries()
      throws InvalidProtocolBufferException {
    QueryRequest queryRequest1 = ExplorerQueries.buildQuery1();
    return Stream.of(
        Arguments.arguments(queryRequest1),
        Arguments.arguments(getAttributeExpressionQuery(queryRequest1)));
  }

  private static Stream<Arguments> provideQueryRequestForAttributeExpressionQueries()
      throws IOException {
    return Stream.of(
        Arguments.arguments(buildQueryFromJsonFile("order_by_query.json"), 10, "server"),
        Arguments.arguments(buildQueryFromJsonFile("group_by_query.json"), 2, "server"),
        Arguments.arguments(buildQueryFromJsonFile("aggregated_function_query.json"), 1, "0.0"));
  }
}
