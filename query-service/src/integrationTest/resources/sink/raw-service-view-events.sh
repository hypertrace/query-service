#!/usr/bin/env bash

# delete if exists
curl -X DELETE http://localhost:8083/connectors/postgres-sink-raw-service-view-events-partv5

# create a new one
curl -i -X PUT -H "Content-Type:application/json" \
    http://localhost:8083/connectors/postgres-sink-raw-service-view-events-partv5/config \
    -d '{
  "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
  "name": "postgres-sink-raw-service-view-events-partv5",
  "input.data.format": "AVRO",
  "connection.url":"jdbc:postgresql://localhost:5432/",
  "connection.host": "localhost",
  "connection.port": "5432",
  "connection.user": "postgres",
  "connection.password": "postgres",
  "db.name": "postgres",
  "dialect.name": "PostgreSqlDatabaseDialect",
  "topics": "raw-service-view-events",
  "insert.mode": "INSERT",
  "db.timezone": "UTC",
  "auto.create": "false",
  "auto.evolve": "false",
  "pk.mode": "none",
  "tasks.max": "1",
  "schema.compatibility": "FULL",
  "fields.whitelist":"customer_id,service_name,api_name,api_is_external,start_time_millis,duration_millis,bytes_received,bytes_sent,error_count,exception_count,protocol_name,status_code,service_id,api_id,num_calls,namespace_name,cluster_name,host_header,api_discovery_state,domain_id,environment"
}'