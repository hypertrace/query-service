package org.hypertrace.core.query.service;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.ManagedChannelBuilder;
import java.io.File;
import java.lang.reflect.Field;
import java.time.Duration;
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
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
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
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.QueryRequest.Builder;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Value;
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
public class QueryServiceTest {

  private static final Logger LOG = LoggerFactory.getLogger(QueryServiceTest.class);
  private static final Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(LOG);
  private static final String tenantId = "__default";

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
  public static void setup() throws Exception{
    network = Network.newNetwork();

    kafkaZk = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))
        .withNetwork(network)
        .withNetworkAliases("kafka", "zookeeper")
        .waitingFor(Wait.forListeningPort());
     //   .withStartupTimeout(Duration.ofMinutes(1));
    kafkaZk.start();

    pinotServiceManager = new GenericContainer<>(
        DockerImageName.parse("hypertrace/pinot-servicemanager:main"))
        .withNetwork(network)
        .withNetworkAliases("pinot-controller", "pinot-server", "pinot-broker")
        .withExposedPorts(8099)
        .withExposedPorts(9000)
        .dependsOn(kafkaZk)
        .waitingFor(Wait.forLogMessage(".*Completed schema installation.*", 1))
       // .withStartupTimeout(Duration.ofMinutes(5))
        .withLogConsumer(logConsumer);
    pinotServiceManager.start();

    mongo = new GenericContainer<>(DockerImageName.parse("hypertrace/mongodb:main"))
        .withNetwork(network)
        .withNetworkAliases("mongo")
        .withExposedPorts(27017)
        .withStartupTimeout(Duration.ofMinutes(1))
        .waitingFor(Wait.forLogMessage(".*waiting for connections on port 27017.*", 1));
    mongo.start();
    mongo.followOutput(logConsumer);

    attributeService = new GenericContainer<>(DockerImageName.parse("traceableai-docker.jfrog.io/hypertrace/attribute-service:test"))
        .withNetwork(network)
        .withNetworkAliases("attribute-service")
        .withEnv("MONGO_HOST", "mongo")
        .withExposedPorts(9012)
        .dependsOn(mongo)
        .withStartupTimeout(Duration.ofMinutes(1))
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
        .withStartupTimeout(Duration.ofMinutes(1))
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

  @Test
  public void test() throws Exception {
    System.out.println("Hello Hello");
    Iterator<ResultSetChunk> result = queryServiceClient.executeQuery(
        buildSimpleQuery(), Map.of("x-tenant-id", tenantId), 2000);

    while (result.hasNext()) {
      ResultSetChunk rsc = result.next();

      System.out.println(rsc);
    }
  }

  private QueryRequest buildSimpleQuery() {
    Builder builder = QueryRequest.newBuilder();
    ColumnIdentifier spanId = ColumnIdentifier.newBuilder().setColumnName("EVENT.id").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(spanId).build());

    Filter startTimeFilter =
        createTimeFilter(
            "EVENT.startTime",
            Operator.GT,
            System.currentTimeMillis() - Duration.ofDays(20).toMillis());
    Filter endTimeFilter =
        createTimeFilter("EVENT.endTime", Operator.LT, System.currentTimeMillis());

    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .build();
    builder.setFilter(andFilter);

    return builder.build();
  }

  private Filter createTimeFilter(String columnName, Operator op, long value) {

    ColumnIdentifier startTimeColumn =
        ColumnIdentifier.newBuilder().setColumnName(columnName).build();
    Expression lhs = Expression.newBuilder().setColumnIdentifier(startTimeColumn).build();

    LiteralConstant constant =
        LiteralConstant.newBuilder()
            .setValue(Value.newBuilder().setString(String.valueOf(value)).build())
            .build();
    Expression rhs = Expression.newBuilder().setLiteral(constant).build();
    return Filter.newBuilder().setLhs(lhs).setOperator(op).setRhs(rhs).build();
  }

  private static boolean bootstrapConfig() throws Exception {
    // set env vars
    setEnv(Map.of(
        "MONGO_PORT", mongo.getMappedPort(27017).toString(),
        "ATTRIBUTE_SERVICE_HOST_CONFIG", attributeService.getHost(),
        "ATTRIBUTE_SERVICE_PORT_CONFIG", attributeService.getMappedPort(9012).toString()
    ));

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

    context.run(
        () ->
            ConfigBootstrapper.main(
                new String[]{
                    "-c",
                    resourcesPath + "/application.conf",
                    "-C",
                    resourcesPath,
                    "--validate",
                    "--upgrade"
                }));

    Channel channel = ManagedChannelBuilder
        .forAddress(attributeService.getHost(), attributeService.getMappedPort(9012)).usePlaintext()
        .build();
    AttributeServiceClient client = new AttributeServiceClient(channel);
    int retry = 0;
    while (Streams.stream(
        client.findAttributes(tenantId, AttributeMetadataFilter.getDefaultInstance()))
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
        .withStartupTimeout(Duration.ofMinutes(1))
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

    KafkaProducer<String, StructuredTrace> producer = new KafkaProducer<>(
        ImmutableMap.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
        ),
        new StringSerializer(),
        new AvroSerde<StructuredTrace>().serializer()
    );
    producer.send(new ProducerRecord<>("enriched-structured-traces", "", trace)).get();

    int retry = 0;
    while (!areMessagesConsumed() && retry++ < 5) {
      Thread.sleep(1000);
    }
    viewGen.stop();
    return retry < 5;
  }

  private static boolean areMessagesConsumed() throws Exception {
    ListConsumerGroupOffsetsResult lcgor = adminClient.listConsumerGroupOffsets("");
    Map<TopicPartition, OffsetAndMetadata> w = lcgor.partitionsToOffsetAndMetadata().get();
    Map<TopicPartition, OffsetSpec> r = Maps.newHashMap();
    w.forEach((k, v) -> r.put(k, OffsetSpec.latest()));
    Map<TopicPartition, ListOffsetsResultInfo> lor = adminClient.listOffsets(r).all().get();
    return lor.entrySet().stream()
        .noneMatch(k -> w.get(k.getKey()).offset() < k.getValue().offset());
  }

  private static void setEnv(Map<String, String> newenv) throws Exception {
    // https://stackoverflow.com/questions/318239/how-do-i-set-environment-variables-from-java
    try {
      Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
      Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
      theEnvironmentField.setAccessible(true);
      Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
      env.putAll(newenv);
      Field theCaseInsensitiveEnvironmentField = processEnvironmentClass
          .getDeclaredField("theCaseInsensitiveEnvironment");
      theCaseInsensitiveEnvironmentField.setAccessible(true);
      Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField
          .get(null);
      cienv.putAll(newenv);
    } catch (NoSuchFieldException e) {
      Class[] classes = Collections.class.getDeclaredClasses();
      Map<String, String> env = System.getenv();
      for (Class cl : classes) {
        if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
          Field field = cl.getDeclaredField("m");
          field.setAccessible(true);
          Object obj = field.get(env);
          Map<String, String> map = (Map<String, String>) obj;
          map.clear();
          map.putAll(newenv);
        }
      }
    }
  }
}



/**
 * System.getProperty("KAFKA_BOOTSTRAP_SERVERS", getBootstrapServers());
 *         try {
 *             execInContainer("/bin/sh", "-c",
 *                     "/usr/bin/kafka-topics --create --topic my-topic-1 --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1");
 *         } catch (UnsupportedOperationException | IOException | InterruptedException e) {
 *             e.printStackTrace();
 *         }
 *
 *          kafkaContainer.execInContainer(
 *                 "/bin/sh",
 *                 "-c",
 *                 "/usr/bin/kafka-topics --create --zookeeper $KAFKA_ZOOKEEPER_CONNECT "
 *                 + "--replication-factor 1 --partitions 1 --topic " + TOPIC);
 *
 *
 *
 *                     Channel channel = ManagedChannelBuilder
 *         .forAddress(attributeService.getHost(), attributeService.getMappedPort(9012)).usePlaintext()
 *         .build();
 *     AttributeServiceClient client = new AttributeServiceClient(channel);
 *     List<AttributeMetadata> attributeMetadataList =
 *         Streams.stream(
 *             client.findAttributes("__default", AttributeMetadataFilter.getDefaultInstance()))
 *             .collect(Collectors.toList());
 *     System.out.println(attributeMetadataList);
 */
// kafkaZk.execInContainer();
