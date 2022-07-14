#!/usr/bin/env bash

# delete if exists
curl -X DELETE http://localhost:8083/connectors/postgres-sink-backend-entity-view-events-partv5

# create a new one
curl -i -X PUT -H "Content-Type:application/json" \
    http://localhost:8083/connectors/postgres-sink-backend-entity-view-events-partv5/config \
    -d '{
  "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
  "name": "postgres-sink-backend-entity-view-events-partv5",
  "input.data.format": "AVRO",
  "connection.url":"jdbc:postgresql://localhost:5432/",
  "connection.host": "localhost",
  "connection.port": "5432",
  "connection.user": "postgres",
  "connection.password": "postgres",
  "db.name": "postgres",
  "dialect.name": "PostgreSqlDatabaseDialect",
  "topics": "backend-entity-view-events",
  "insert.mode": "INSERT",
  "db.timezone": "UTC",
  "auto.create": "false",
  "auto.evolve": "false",
  "pk.mode": "none",
  "tasks.max": "1",
  "schema.compatibility": "FULL",
  "transforms": "valueToJson",
  "transforms.valueToJson.fields": "attribute_map, request_headers,request_params,response_headers,tags, x-",
  "transforms.valueToJson.type": "ai.traceable.kafka.connect.smt.RecordFieldToJsonString$Value",
  "fields.whitelist": "customer_id,backend_id,backend_host,backend_port,backend_protocol,backend_path,attribute_map,start_time_millis,end_time_millis,duration_millis,bytes_received,bytes_sent,span_kind,error_count,exception_count,num_calls,backend_name,backend_trace_id,display_name,request_headers,request_params,response_headers,request_body_type,response_body_type,status_code,status_message,status,tags,caller_service_id,caller_api_id,environment,backend_operation,backend_destination,is_bare,is_sampled"
}'