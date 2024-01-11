package org.hypertrace.core.query.service.trino;

import static io.trino.client.JsonCodec.jsonCodec;
import static io.trino.client.StatementClientFactory.newStatementClient;

import io.airlift.units.Duration;
import io.trino.client.ClientSession;
import io.trino.client.JsonCodec;
import io.trino.client.JsonResponse;
import io.trino.client.QueryResults;
import io.trino.client.StatementClient;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.Date;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import okhttp3.Call;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;

public class TrinoRestDriverApp {
  private static final MediaType MEDIA_TYPE_TEXT = MediaType.parse("text/plain; charset=utf-8");

  public static void main(String[] args) throws URISyntaxException, SQLException {
    createView();
    readView();
    readView();
    dropView();
  }

  private static void createView() throws URISyntaxException {
    // based on AbstractTestingTrinoClient.execute()
    OkHttpClient httpClient = new OkHttpClient();
    ClientSession clientSession = buildClientSession();
    String viewSql = "CREATE MATERIALIZED VIEW IF NOT EXISTS span_count_112 AS "
        + "select count(*) as count from span_event_view limit 1";
    try (StatementClient client = newStatementClient((Call.Factory) httpClient, clientSession, viewSql)) {
      while (client.getStats().getState().equalsIgnoreCase("QUEUED")) {
        //System.out.println(new Date() + " current status info: " + client.currentStatusInfo());
        System.out.println(new Date() + " current state: " + client.getStats().getState());
        client.advance();
      }
    }
  }

  private static void readView() throws SQLException {
    System.out.println(new Date() + " starting to read view");

    String url = "jdbc:trino://localhost:8080/iceberg/iceberg_gcs";
    Properties properties = new Properties();
    properties.setProperty("user", "admin");
    properties.setProperty("password", "");
    Connection connection = DriverManager.getConnection(url, properties);
    Statement statement = connection.createStatement();

    long startTimeMillis = System.currentTimeMillis();
    ResultSet resultSet =
        statement.executeQuery("Select count from span_count_112 limit 1");
    long endTimeMillis = System.currentTimeMillis();

    int total = 0;
    while (resultSet.next()) {
      int count = resultSet.getInt("count");
      System.out.printf(new Date() +  " count: %d%n", count);
      total++;
    }
    System.out.printf(new Date() + " total rows: %d time: %d\n", total, (endTimeMillis - startTimeMillis)/1000);
  }

  private static void dropView() throws SQLException {
    String url = "jdbc:trino://localhost:8080/iceberg/iceberg_gcs";
    Properties properties = new Properties();
    properties.setProperty("user", "admin");
    properties.setProperty("password", "");
    Connection connection = DriverManager.getConnection(url, properties);
    Statement statement = connection.createStatement();

    long startTimeMillis = System.currentTimeMillis();
    int updateCount =
        statement.executeUpdate("DROP MATERIALIZED VIEW IF EXISTS span_count_112");
    long endTimeMillis = System.currentTimeMillis();
    System.out.printf("dropView updateCount: %d time: %d\n", updateCount, (endTimeMillis - startTimeMillis)/1000);
  }

  public static void runEndToEnd(String[] args) throws URISyntaxException {
    // based on AbstractTestingTrinoClient.execute()
    OkHttpClient httpClient = new OkHttpClient();
    ClientSession clientSession = buildClientSession();
    // String sql = "select count(*) from span_event_view limit 5";
    String viewSql = "CREATE MATERIALIZED VIEW IF NOT EXISTS span_count_100 AS "
        + "select count(*) as count from span_event_view limit 1";
    try (StatementClient client = newStatementClient((Call.Factory) httpClient, clientSession, viewSql)) {
      while (client.isRunning()) {
        System.out.println("current status info: " + client.currentStatusInfo());
        System.out.println("current data: " + client.currentData().getData());
        client.advance();
      }
      System.out.println("query finished now");
      System.out.println("final status info: " + client.finalStatusInfo());
    }
  }

  private static void useJsonResponse() {
    OkHttpClient httpClient = new OkHttpClient();
    JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    Request request = buildPostQueryRequest(
        "http://localhost:8080/v1/statement",
        "select count(*) from span_event_view limit 5");
    JsonResponse<QueryResults> response =
        JsonResponse.execute(QUERY_RESULTS_CODEC, (Call.Factory) httpClient, request, OptionalLong.empty());
    System.out.println("response status code: " + response.getStatusCode());
    System.out.println("response contains value: " + response.hasValue());
    System.out.println("query results: " + response.getValue());
  }
  private static ClientSession buildClientSession() throws URISyntaxException {
    return ClientSession.builder()
        .server(new URI("http://localhost:8080"))
        .user(Optional.of("admin"))
        .catalog("iceberg")
        .schema("iceberg_gcs")
        .timeZone(ZoneId.of("UTC"))
        .clientRequestTimeout(new Duration(1, TimeUnit.DAYS))
        .build();
  }

  private static Request buildPostQueryRequest(String urlStr, String query) {
    HttpUrl url = HttpUrl.get(urlStr);
    return new Request.Builder()
        .url(url)
        .post(RequestBody.create(MEDIA_TYPE_TEXT, query))
        .addHeader("X-Trino-User", "admin")
        .addHeader("X-Trino-Time-Zone", "UTC")
        .addHeader("X-Trino-Catalog", "iceberg")
        .addHeader("X-Trino-Schema", "iceberg_gcs")
        .addHeader("User-Agent", "trino-cli")
        .build();
  }
}
