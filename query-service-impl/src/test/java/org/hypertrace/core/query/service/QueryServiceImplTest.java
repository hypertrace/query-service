package org.hypertrace.core.query.service;

import com.google.common.collect.Lists;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.QueryRequest.Builder;
import org.hypertrace.core.query.service.api.QueryServiceGrpc;
import org.hypertrace.core.query.service.api.QueryServiceGrpc.QueryServiceBlockingStub;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.util.QueryRequestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryServiceImplTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryServiceImplTest.class);

  @Test
  public void testQueryServiceImplInitialization() {
    QueryServiceImplConfig queryServiceConfig = new QueryServiceImplConfig();
    queryServiceConfig.setClients(List.of());
    queryServiceConfig.setQueryRequestHandlersConfig(List.of());

    QueryServiceImpl queryServiceImpl = new QueryServiceImpl(queryServiceConfig);
    Assertions.assertNotNull(queryServiceImpl);
  }

  @Test
  public void testBlankTenantColumnNameThrowsException() {
    // Empty tenant id column name
    QueryServiceImplConfig queryServiceConfig = new QueryServiceImplConfig();
    queryServiceConfig.setClients(List.of());

    Assertions.assertThrows(
        RuntimeException.class,
        () -> new QueryServiceImpl(queryServiceConfig),
        "Tenant column name is not defined. Need to set service.config.tenantColumnName in the application config.");

    // null tenant id column name
    QueryServiceImplConfig queryServiceConfig1 = new QueryServiceImplConfig();
    queryServiceConfig1.setClients(List.of());

    Assertions.assertThrows(
        RuntimeException.class,
        () -> new QueryServiceImpl(queryServiceConfig1),
        "Tenant column name is not defined. Need to set service.config.tenantColumnName in the application config.");

    // whitespace tenant id column name
    QueryServiceImplConfig queryServiceConfig2 = new QueryServiceImplConfig();
    queryServiceConfig2.setClients(List.of());

    Assertions.assertThrows(
        RuntimeException.class,
        () -> new QueryServiceImpl(queryServiceConfig2),
        "Tenant column name is not defined. Need to set service.config.tenantColumnName in the application config.");
  }

  // works with query service running at localhost
  @Disabled
  public void testGrpc() {
    ManagedChannel managedChannel =
        ManagedChannelBuilder.forAddress("localhost", 8090).usePlaintext().build();
    QueryServiceBlockingStub QueryServiceBlockingStub =
        QueryServiceGrpc.newBlockingStub(managedChannel);

    ArrayList<QueryRequest> queryRequests =
        Lists.newArrayList(
            buildSimpleQuery(),
            buildAggQuery(),
            buildGroupByAggQuery(),
            buildGroupByTimeRollupAggQuery());

    for (QueryRequest queryRequest : queryRequests) {
      LOGGER.info("Trying to send request {}", queryRequest);
      Iterator<ResultSetChunk> resultSetChunkIterator =
          QueryServiceBlockingStub.withDeadline(Deadline.after(15, TimeUnit.SECONDS))
              .execute(queryRequest);
      LOGGER.info("Got response back: {}", resultSetChunkIterator);
      while (resultSetChunkIterator.hasNext()) {
        LOGGER.info("{}", resultSetChunkIterator.next());
      }
    }
  }

  @Disabled
  public void testGrpcMap() {
    ManagedChannel managedChannel =
        ManagedChannelBuilder.forAddress("localhost", 8090).usePlaintext().build();
    QueryServiceBlockingStub QueryServiceBlockingStub =
        QueryServiceGrpc.newBlockingStub(managedChannel);

    ArrayList<QueryRequest> queryRequests = Lists.newArrayList(buildSimpleMapQuery());

    for (QueryRequest queryRequest : queryRequests) {
      LOGGER.info("Trying to send request {}", queryRequest);
      Iterator<ResultSetChunk> resultSetChunkIterator =
          QueryServiceBlockingStub.withDeadline(Deadline.after(25, TimeUnit.SECONDS))
              .execute(queryRequest);
      LOGGER.info("Got response back: {}", resultSetChunkIterator);
      while (resultSetChunkIterator.hasNext()) {
        LOGGER.info(" Result {}", resultSetChunkIterator.next());
      }
    }
  }

  private QueryRequest buildSimpleQuery() {
    Builder builder = QueryRequest.newBuilder();
    ColumnIdentifier spanId = ColumnIdentifier.newBuilder().setColumnName("EVENT.id").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(spanId).build());

    Filter startTimeFilter =
        createTimeFilter(
            "EVENT.start_time_millis",
            Operator.GT,
            System.currentTimeMillis() - 1000 * 60 * 60 * 24);
    Filter endTimeFilter =
        createTimeFilter("EVENT.end_time_millis", Operator.LT, System.currentTimeMillis());

    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .build();
    builder.setFilter(andFilter);

    return builder.build();
  }

  private QueryRequest buildGroupByAggQuery() {
    Builder builder = QueryRequest.newBuilder();
    builder.addAggregation(QueryRequestUtil.createCountByColumnSelection("EVENT.id"));

    Filter startTimeFilter =
        createTimeFilter(
            "EVENT.start_time_millis",
            Operator.GT,
            System.currentTimeMillis() - 1000 * 60 * 60 * 24);
    Filter endTimeFilter =
        createTimeFilter("EVENT.end_time_millis", Operator.LT, System.currentTimeMillis());

    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .build();
    builder.setFilter(andFilter);

    builder.addGroupBy(
        Expression.newBuilder()
            .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("EVENT.displaySpanName").build()));
    return builder.build();
  }

  private QueryRequest buildGroupByTimeRollupAggQuery() {
    Builder builder = QueryRequest.newBuilder();
    builder.addAggregation(QueryRequestUtil.createCountByColumnSelection("EVENT.id"));

    Filter startTimeFilter =
        createTimeFilter(
            "EVENT.start_time_millis",
            Operator.GT,
            System.currentTimeMillis() - 1000 * 60 * 60 * 24);
    Filter endTimeFilter =
        createTimeFilter("EVENT.end_time_millis", Operator.LT, System.currentTimeMillis());

    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .build();
    builder.setFilter(andFilter);

    Function groupByTimeUdf =
        Function.newBuilder()
            .setFunctionName("dateTimeConvert")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder()
                            .setColumnName("EVENT.start_time_millis")
                            .build()))
            .addArguments(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder().setString("1:MILLISECONDS:EPOCH").build())))
            .addArguments(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder().setString("1:MILLISECONDS:EPOCH").build())))
            .addArguments(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(Value.newBuilder().setString("30:SECONDS").build())))
            .build();
    builder.addGroupBy(Expression.newBuilder().setFunction(groupByTimeUdf).build());
    return builder.build();
  }

  private QueryRequest buildSimpleMapQuery() {
    Builder builder = QueryRequest.newBuilder();
    ColumnIdentifier spanId =
        ColumnIdentifier.newBuilder().setColumnName("EVENT.id").setAlias("SpanIds").build();
    builder.addSelection(Expression.newBuilder().setColumnIdentifier(spanId).build());
    builder.addSelection(createSelection("EVENT.end_time_millis"));
    builder.addSelection(createSelection("EVENT.displaySpanName"));
    builder.addSelection(createSelection("EVENT.attributes.request_body"));
    builder.addSelection(createSelection("EVENT.attributes.protocol_name"));
    builder.addSelection(createSelection("EVENT.attributes.request_headers"));
    builder.addSelection(createSelection("EVENT.attributes.response_headers"));

    builder.addSelection(createSelection("EVENT.start_time_millis"));
    builder.addSelection(createSelection("EVENT.metrics.duration_millis"));
    builder.addSelection(createSelection("Service.name"));
    builder.addSelection(createSelection("EVENT.attributes.response_body"));
    builder.addSelection(createSelection("EVENT.attributes.parent_span_id"));

    Filter startTimeFilter =
        createTimeFilter(
            "EVENT.start_time_millis",
            Operator.GT,
            System.currentTimeMillis() - 1000 * 60 * 60 * 24);
    Filter endTimeFilter =
        createTimeFilter("EVENT.end_time_millis", Operator.LT, System.currentTimeMillis());

    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .build();

    builder.setFilter(andFilter);

    return builder.build();
  }

  private QueryRequest buildAggQuery() {
    Builder builder = QueryRequest.newBuilder();
    Function maxStartTime =
        Function.newBuilder()
            .setFunctionName("MAX")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder()
                            .setColumnName("EVENT.start_time_millis")
                            .build()))
            .setAlias("MAX_start_time_millis")
            .build();

    builder.addSelection(createSelection("EVENT.attributes.request_headers"));
    builder.addSelection(Expression.newBuilder().setFunction(maxStartTime).build());

    Filter startTimeFilter =
        createTimeFilter(
            "EVENT.start_time_millis",
            Operator.GT,
            System.currentTimeMillis() - 1000 * 60 * 60 * 24);
    Filter endTimeFilter =
        createTimeFilter("EVENT.end_time_millis", Operator.LT, System.currentTimeMillis());

    Filter andFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(startTimeFilter)
            .addChildFilter(endTimeFilter)
            .build();
    builder.setFilter(andFilter);

    return builder.build();
  }

  private Expression createSelection(String colName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(colName))
        .build();
  }

  private Filter createTimeFilter(String columnName, Operator op, long value) {

    ColumnIdentifier startTimeColumn =
        ColumnIdentifier.newBuilder().setColumnName(columnName).build();
    Expression lhs = Expression.newBuilder().setColumnIdentifier(startTimeColumn).build();

    // TODO: Why is this not LONG
    LiteralConstant constant =
        LiteralConstant.newBuilder()
            .setValue(Value.newBuilder().setString(String.valueOf(value)).build())
            .build();
    Expression rhs = Expression.newBuilder().setLiteral(constant).build();
    return Filter.newBuilder().setLhs(lhs).setOperator(op).setRhs(rhs).build();
  }

  private Filter createStringColumnFilter(String columnName, Operator op, String value) {
    ColumnIdentifier column = ColumnIdentifier.newBuilder().setColumnName(columnName).build();
    Expression lhs = Expression.newBuilder().setColumnIdentifier(column).build();

    LiteralConstant constant =
        LiteralConstant.newBuilder()
            .setValue(Value.newBuilder().setString(String.valueOf(value)).build())
            .build();
    Expression rhs = Expression.newBuilder().setLiteral(constant).build();
    return Filter.newBuilder().setLhs(lhs).setOperator(op).setRhs(rhs).build();
  }
}
