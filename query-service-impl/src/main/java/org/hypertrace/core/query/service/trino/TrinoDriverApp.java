package org.hypertrace.core.query.service.trino;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class TrinoDriverApp {

  public static void main(String[] args) throws SQLException {
    // properties
    String url = "jdbc:trino://localhost:8080/iceberg/iceberg_gcs";
    Properties properties = new Properties();
    properties.setProperty("user", "admin");
    properties.setProperty("password", "");
    //properties.setProperty("SSL", "true");
    Connection connection = DriverManager.getConnection(url, properties);
    Statement statement = connection.createStatement();

    createView(statement);
    refreshView(statement);

    // fetch results twice from view
    getResult(statement);
    getResult(statement);

    dropView(statement);
  }

  private static void createView(Statement statement) throws SQLException {
    long startTimeMillis = System.currentTimeMillis();
    int updateCount =
        statement.executeUpdate(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS per_api_span_count2 AS "
                + "Select api_id, count(*) as count "
                + "from span_event_view "
                + "where customer_id = '3e761879-c77b-4d8f-a075-62ff28e8fa8a' "
                + "and start_time_millis > 1698822000000 "
                + "and regexp_like(request_body, '.*id.*') and regexp_like(response_body, '.*id.*') "
                + "group by api_id order by count DESC limit 1000");
    long endTimeMillis = System.currentTimeMillis();
    System.out.printf("createView updateCount: %d time: %d\n", updateCount, (endTimeMillis - startTimeMillis)/1000);
  }

  private static void refreshView(Statement statement) throws SQLException {
    long startTimeMillis = System.currentTimeMillis();
    int updateCount =
        statement.executeUpdate("REFRESH MATERIALIZED VIEW per_api_span_count2");
    long endTimeMillis = System.currentTimeMillis();
    System.out.printf("refreshView updateCount: %d time: %d\n", updateCount, (endTimeMillis - startTimeMillis)/1000);
  }

  private static void dropView(Statement statement) throws SQLException {
    long startTimeMillis = System.currentTimeMillis();
    int updateCount =
        statement.executeUpdate("DROP MATERIALIZED VIEW IF EXISTS per_api_span_count1");
    long endTimeMillis = System.currentTimeMillis();
    System.out.printf("dropView updateCount: %d time: %d\n", updateCount, (endTimeMillis - startTimeMillis)/1000);
  }
  private static void getResult(Statement statement) throws SQLException {
    long startTimeMillis = System.currentTimeMillis();
    ResultSet resultSet =
        statement.executeQuery("Select api_id, count from per_api_span_count2 limit 10");
    long endTimeMillis = System.currentTimeMillis();

    String api_id, api_name, service_name, service_id = null;
    int count = 0;
    int total = 0;
    while (resultSet.next()) {
      api_id = resultSet.getString("api_id");
//      api_name = resultSet.getString("api_name");
//      service_id = resultSet.getString("service_id");
//      service_name = resultSet.getString("service_name");
      count = resultSet.getInt("count");
      //          String.format("%s, %s, %s, %s, %d", api_id, api_name, service_id, service_name, count));
      System.out.printf("api_id: %s count: %d%n", api_id, count);
      total++;
    }
    System.out.printf("total rows: %d time: %d\n", total, (endTimeMillis - startTimeMillis)/1000);
  }
}
