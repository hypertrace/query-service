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
    ResultSet resultSet =
        statement.executeQuery("Select api_id, api_name, service_id, service_name, COUNT(*) as count "
            + "from span_event_view_staging_test "
            + "where customer_id = 'b227d0f9-98e1-4eff-acf5-ab129d416914' "
            + "and start_time_millis >= 1692943200000  and start_time_millis < 1692946800000 "
            + "AND api_id != 'null' AND api_discovery_state IN ('DISCOVERED', 'UNDER_DISCOVERY') "
            + "GROUP BY api_id, api_name, service_name, service_id limit 10000");

    String api_id, api_name, service_name, service_id = null;
    int count = 0;
    int total = 0;
    while (resultSet.next()) {
      api_id = resultSet.getString("api_id");
      api_name = resultSet.getString("api_name");
      service_id = resultSet.getString("service_id");
      service_name = resultSet.getString("service_name");
      count = resultSet.getInt("count");
      System.out.println(String.format("%s, %s, %s, %s, %d", api_id, api_name, service_id, service_name, count));
      total++;
    }
    System.out.println("total: " + total);
  }
}
