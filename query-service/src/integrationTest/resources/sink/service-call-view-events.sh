#!/usr/bin/env bash

# delete if exists
curl -X DELETE http://localhost:8083/connectors/postgres-sink-service-call-view-events-partv5

# create a new one
curl -i -X PUT -H "Content-Type:application/json" \
    http://localhost:8083/connectors/postgres-sink-service-call-view-events-partv5/config \
    -d '{
  "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
  "name": "postgres-sink-service-call-view-events-partv5",
  "input.data.format": "AVRO",
  "connection.url":"jdbc:postgresql://localhost:5432/",
  "connection.host": "localhost",
  "connection.port": "5432",
  "connection.user": "postgres",
  "connection.password": "postgres",
  "db.name": "postgres",
  "dialect.name": "PostgreSqlDatabaseDialect",
  "topics": "service-call-view-events",
  "insert.mode": "INSERT",
  "db.timezone": "UTC",
  "auto.create": "false",
  "auto.evolve": "false",
  "pk.mode": "none",
  "tasks.max": "1",
  "schema.compatibility": "FULL",
  "fields.whitelist":"customer_id,trace_id,transaction_name,client_event_id,server_event_id,caller_service,caller_api,callee_namespace,callee_service,callee_api,request_url,request_method,protocol_name,response_status_code,start_time_millis,duration_millis,total_duration_millis,server_bytes_received,server_bytes_sent,client_bytes_sent,client_bytes_received,error_count,exception_count,caller_service_id_str,caller_api_id_str,callee_service_id_str,callee_api_id_str,num_calls,caller_namespace,callee_backend_id,callee_backend_name,caller_cluster_name,callee_cluster_name,caller_environment,callee_environment"
}'