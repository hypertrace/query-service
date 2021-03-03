package org.hypertrace.core.query.service.htqueries;

import static com.github.stefanbirkner.systemlambda.SystemLambda.withEnvironmentVariable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.ManagedChannelBuilder;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.hypertrace.core.attribute.service.client.AttributeServiceClient;
import org.hypertrace.core.attribute.service.v1.AttributeMetadataFilter;
import org.hypertrace.core.bootstrapper.ConfigBootstrapper;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.grpcutils.context.RequestContextConstants;
import org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.core.query.service.client.QueryServiceConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

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
  private static GenericContainer<?> queryService;

  private static QueryServiceClient queryServiceClient;

  @BeforeAll
  public static void setup() throws Exception {
    network = Network.newNetwork();

    kafkaZk = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))
        .withNetwork(network)
        .withNetworkAliases("kafka", "zookeeper")
        .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
        .waitingFor(Wait.forListeningPort());
    kafkaZk.start();

    pinotServiceManager = new GenericContainer<>(
        DockerImageName.parse("hypertrace/pinot-servicemanager:main"))
        .withNetwork(network)
        .withNetworkAliases("pinot-controller", "pinot-server", "pinot-broker")
        .withExposedPorts(8099)
        .withExposedPorts(9000)
        .dependsOn(kafkaZk)
        .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
        .waitingFor(Wait.forLogMessage(".*Completed schema installation.*", 1))
        .withLogConsumer(logConsumer);
    pinotServiceManager.start();

    mongo = new GenericContainer<>(DockerImageName.parse("hypertrace/mongodb:main"))
        .withNetwork(network)
        .withNetworkAliases("mongo")
        .withExposedPorts(27017)
        .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
        .waitingFor(Wait.forLogMessage(".*waiting for connections on port 27017.*", 1));
    mongo.start();
    mongo.followOutput(logConsumer);

    attributeService = new GenericContainer<>(
        DockerImageName.parse("hypertrace/attribute-service:main"))
        .withNetwork(network)
        .withNetworkAliases("attribute-service")
        .withEnv("MONGO_HOST", "mongo")
        .withExposedPorts(9012)
        .dependsOn(mongo)
        .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
        .waitingFor(Wait.forLogMessage(".*Started admin service on port: 9013.*", 1));
    attributeService.start();
    attributeService.followOutput(logConsumer);

    queryService = new GenericContainer<>(DockerImageName.parse("hypertrace/query-service:main"))
        .withNetwork(network)
        .withNetworkAliases("query-service")
        .withEnv("ATTRIBUTE_SERVICE_HOST_CONFIG", attributeService.getHost())
        .withEnv("ATTRIBUTE_SERVICE_PORT_CONFIG", attributeService.getMappedPort(9012).toString())
        .withEnv("ZK_CONNECT_STR", "zookeeper:2181/hypertrace-views")
        .withExposedPorts(8090)
        .dependsOn(pinotServiceManager)
        .dependsOn(attributeService)
        .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
        .waitingFor(Wait.forLogMessage(".*Started admin service on port: 8091.*", 1));
    queryService.start();
    queryService.followOutput(logConsumer);

    List<String> topicsNames = List.of(
        "enriched-structured-traces",
        "raw-service-view-events",
        "raw-trace-view-events",
        "service-call-view-events",
        "span-event-view",
        "backend-entity-view-events");
    bootstrapServers = kafkaZk.getBootstrapServers();
    adminClient = AdminClient
        .create(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaZk.getBootstrapServers()));
    List<NewTopic> topics = topicsNames.stream().map(v -> new NewTopic(v, 1, (short) 1))
        .collect(Collectors.toList());
    adminClient.createTopics(topics);

    assertTrue(bootstrapConfig());
    LOG.info("Bootstrap Complete");
    assertTrue(generateData());
    LOG.info("Generate Data Complete");

    Map<String, Object> map = Maps.newHashMap();
    map.put("host", queryService.getHost());
    map.put("port", queryService.getMappedPort(8090));
    QueryServiceConfig queryServiceConfig = new QueryServiceConfig(ConfigFactory.parseMap(map));
    queryServiceClient = new QueryServiceClient(queryServiceConfig);
  }

  @AfterAll
  public static void shutdown() {
    LOG.info("Initiating shutdown");
    queryService.stop();
    attributeService.stop();
    mongo.stop();
    pinotServiceManager.stop();
    kafkaZk.stop();
    network.close();
  }

  private static boolean bootstrapConfig() throws Exception {
    String resourcesPath =
        Thread.currentThread()
            .getContextClassLoader()
            .getResource("config-bootstrapper")
            .getPath();

    // Since the clients to run Config commands are created internal to this code,
    // we need to set the tenantId in the context so that it's propagated.
    RequestContext requestContext = new RequestContext();
    requestContext.add(RequestContextConstants.TENANT_ID_HEADER_KEY, "__default");
    Context context = Context.current().withValue(RequestContext.CURRENT, requestContext);

    withEnvironmentVariable(
        "MONGO_PORT", mongo.getMappedPort(27017).toString())
        .and("ATTRIBUTE_SERVICE_HOST_CONFIG", attributeService.getHost())
        .and("ATTRIBUTE_SERVICE_PORT_CONFIG", attributeService.getMappedPort(9012).toString())
        .execute(() -> context.run(
            () -> ConfigBootstrapper.main(
                new String[]{
                    "-c",
                    resourcesPath + "/application.conf",
                    "-C",
                    resourcesPath,
                    "--validate",
                    "--upgrade"
                })));

    Channel channel = ManagedChannelBuilder
        .forAddress(attributeService.getHost(), attributeService.getMappedPort(9012)).usePlaintext()
        .build();
    AttributeServiceClient client = new AttributeServiceClient(channel);
    int retry = 0;
    while (Streams.stream(
        client.findAttributes(TENANT_ID_MAP, AttributeMetadataFilter.getDefaultInstance()))
        .collect(Collectors.toList()).size() == 0 && retry++ < 5) {
      Thread.sleep(1000);
    }
    return retry < 5;
  }

  private static boolean generateData() throws Exception {
    // start view-gen service
    GenericContainer<?> viewGen = new GenericContainer(
        DockerImageName.parse("hypertrace/hypertrace-view-generator:main"))
        .withNetwork(network)
        .dependsOn(kafkaZk)
        .withEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        .withCopyFileToContainer(
            MountableFile.forHostPath(
                Thread.currentThread().getContextClassLoader().getResource("view-generator")
                    .getPath() + "/application.conf"),
            "/app/resources/configs/all-views/application.conf")
        .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
        .waitingFor(Wait.forLogMessage(".* Started admin service on port: 8099.*", 1));
    viewGen.start();
    viewGen.followOutput(logConsumer);

    // produce data
    SpecificDatumReader<StructuredTrace> datumReader = new SpecificDatumReader<>(
        StructuredTrace.getClassSchema());

    DataFileReader<StructuredTrace> dfrStructuredTrace = new DataFileReader<>(
        new File(Thread.currentThread()
            .getContextClassLoader()
            .getResource("StructuredTrace-Hotrod.avro").getPath()),
        datumReader);

    StructuredTrace trace = dfrStructuredTrace.next();
    dfrStructuredTrace.close();

    updateTraceTimeStamp(trace);
    KafkaProducer<String, StructuredTrace> producer = new KafkaProducer<>(
        ImmutableMap.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
        ),
        new StringSerializer(),
        new AvroSerde<StructuredTrace>().serializer()
    );
    producer.send(new ProducerRecord<>("enriched-structured-traces", "", trace)).get();

    Map<String, Long> endOffSetMap = Map.of(
        "raw-service-view-events", 13L,
        "backend-entity-view-events", 11L,
        "raw-trace-view-events", 1L,
        "service-call-view-events", 27L,
        "span-event-view", 50L);
    int retry = 0;
    while (!areMessagesConsumed(endOffSetMap) && retry++ < 5) {
      Thread.sleep(2000);
    }
    // stop this service
    viewGen.stop();

    return retry < 5;
  }

  private static boolean areMessagesConsumed(Map<String, Long> endOffSetMap) throws Exception {
    ListConsumerGroupOffsetsResult consumerGroupOffsetsResult = adminClient
        .listConsumerGroupOffsets("");
    Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap =
        consumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get();
    if (offsetAndMetadataMap.size() < 5) {
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
    trace.getEntityEdgeList()
        .forEach(edge -> edge.setStartTimeMillis(edge.getStartTimeMillis() + delta));
    trace.getEntityEdgeList()
        .forEach(edge -> edge.setEndTimeMillis(edge.getEndTimeMillis() + delta));
    trace.getEventEdgeList()
        .forEach(edge -> edge.setStartTimeMillis(edge.getStartTimeMillis() + delta));
    trace.getEventEdgeList()
        .forEach(edge -> edge.setEndTimeMillis(edge.getEndTimeMillis() + delta));
    trace.getEntityEventEdgeList()
        .forEach(edge -> edge.setStartTimeMillis(edge.getStartTimeMillis() + delta));
    trace.getEntityEventEdgeList()
        .forEach(edge -> edge.setEndTimeMillis(edge.getEndTimeMillis() + delta));
  }

  @Test
  public void testServicesQueries() {
    LOG.info("Services queries");
    Iterator<ResultSetChunk> itr = queryServiceClient.executeQuery(
        ServicesQueries.buildQuery1(), TENANT_ID_MAP, 10000);
    List<ResultSetChunk> list = Streams.stream(itr).collect(Collectors.toList());
    List<Row> rows = list.get(0).getRowList();
    assertEquals(4, rows.size());
    List<String> serviceNames = new ArrayList<>(
        Arrays.asList("frontend", "driver", "route", "customer"));
    rows.forEach(row -> serviceNames.remove(row.getColumn(1).getString()));
    assertTrue(serviceNames.isEmpty());
  }

  @Test
  public void testBackendsQueries() {
    LOG.info("Backends queries");
    Iterator<ResultSetChunk> itr = queryServiceClient.executeQuery(
        BackendsQueries.buildQuery1(), TENANT_ID_MAP, 10000);
    List<ResultSetChunk> list = Streams.stream(itr).collect(Collectors.toList());
    List<Row> rows = list.get(0).getRowList();
    assertEquals(1, rows.size());
    List<String> backendNames = new ArrayList<>(Collections.singletonList("redis"));
    rows.forEach(row -> backendNames.remove(row.getColumn(1).getString()));
    assertTrue(backendNames.isEmpty());
  }

  @Test
  public void testExplorerQueries() {
    LOG.info("Explorer queries");
    Iterator<ResultSetChunk> itr = queryServiceClient.executeQuery(
        ExplorerQueries.buildQuery1(), TENANT_ID_MAP, 10000);
    List<ResultSetChunk> list = Streams.stream(itr).collect(Collectors.toList());
    List<Row> rows = list.get(0).getRowList();
    assertEquals(1, rows.size());
    // COUNT_API_TRACE.calls_[] is 13
    assertEquals("13", rows.get(0).getColumn(1).getString());
  }
}