#!/usr/bin/env bash

# delete if exists
curl -X DELETE http://localhost:8083/connectors/postgres-sink-raw-trace-view-events-partv5

# create a new one
curl -i -X PUT -H "Content-Type:application/json" \
    http://localhost:8083/connectors/postgres-sink-raw-trace-view-events-partv5/config \
    -d '{
  "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
  "name": "postgres-sink-raw-trace-view-events-partv5",
  "input.data.format": "AVRO",
  "connection.url":"jdbc:postgresql://localhost:5432/",
  "connection.host": "localhost",
  "connection.port": "5432",
  "connection.user": "postgres",
  "connection.password": "postgres",
  "db.name": "postgres",
  "dialect.name": "PostgreSqlDatabaseDialect",
  "topics": "raw-trace-view-events",
  "insert.mode": "INSERT",
  "db.timezone": "UTC",
  "auto.create": "false",
  "auto.evolve": "false",
  "pk.mode": "none",
  "tasks.max": "1",
  "schema.compatibility": "FULL",
  "transforms": "valueToJson",
  "transforms.valueToJson.fields": "tags",
  "transforms.valueToJson.type": "ai.traceable.kafka.connect.smt.RecordFieldToJsonString$Value",
  "fields.whitelist":"customer_id,trace_id,transaction_name,services,start_time_millis,end_time_millis,duration_millis,num_services,num_spans,tags,environments"
}'