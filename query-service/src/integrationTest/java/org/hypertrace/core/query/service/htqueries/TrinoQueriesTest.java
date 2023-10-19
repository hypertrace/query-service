package org.hypertrace.core.query.service.htqueries;

import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.core.query.service.client.QueryServiceConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TrinoQueriesTest {
  private static final Map<String, String> TENANT_ID_MAP =
      Map.of("x-tenant-id", "3e761879-c77b-4d8f-a075-62ff28e8fa8a");
  private static QueryServiceClient queryServiceClient;

  @BeforeAll
  public static void setUp() {
    Map<String, Object> map = Maps.newHashMap();
    map.put("host", "localhost");
    map.put("port", 8090);
    QueryServiceConfig queryServiceConfig = new QueryServiceConfig(ConfigFactory.parseMap(map));
    queryServiceClient = new QueryServiceClient(queryServiceConfig);
  }

  @Test
  public void testTrinoQueries() {
    long startTimeMillis = System.currentTimeMillis();
    System.out.println(new Date() + " Starting query");
    Iterator<ResultSetChunk> itr =
        queryServiceClient.executeQuery(ExplorerQueries.buildQuery2(), TENANT_ID_MAP, 600000);
    List<ResultSetChunk> resultSetChunks = Streams.stream(itr).collect(Collectors.toList());

    int total = 0;
    resultSetChunks.forEach(
        chunk -> {
          List<Row> rows = chunk.getRowList();
          rows.forEach(
              row -> {
                //                String id = row.getColumn(0).getString();
                //                String api_name = row.getColumn(1).getString();
                //                String service_id = row.getColumn(2).getString();
                //                String service_name = row.getColumn(3).getString();
                String count = row.getColumn(0).getString();
                //                System.out.printf("%s, %s, %s, %s%n", id, api_name, service_id,
                // service_name);
                System.out.println(new Date() + " count: " + count);
              });
        });
    long endTimeMillis = System.currentTimeMillis();
    // assertEquals(20, rows.size());
    System.out.println(new Date() +
        " total rows: "
            + resultSetChunks.stream().mapToInt(chunk -> chunk.getRowList().size()).sum());
    System.out.println("Total time seconds: " + (endTimeMillis - startTimeMillis)/1000);
  }
}
