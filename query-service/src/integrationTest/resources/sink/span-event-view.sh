#!/usr/bin/env bash

# delete if exists
curl -X DELETE http://localhost:8083/connectors/postgres-sink-span-event-view-partv5

# create a new one
curl -i -X PUT -H "Content-Type:application/json" \
    http://localhost:8083/connectors/postgres-sink-span-event-view-partv5/config \
    -d '{
  "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
  "name": "postgres-sink-span-event-view-partv5",
  "input.data.format": "AVRO",
  "connection.url":"jdbc:postgresql://localhost:5432/",
  "connection.host": "localhost",
  "connection.port": "5432",
  "connection.user": "postgres",
  "connection.password": "postgres",
  "db.name": "postgres",
  "dialect.name": "PostgreSqlDatabaseDialect",
  "topics": "span-event-view",
  "insert.mode": "INSERT",
  "db.timezone": "UTC",
  "auto.create": "false",
  "auto.evolve": "false",
  "pk.mode": "none",
  "tasks.max": "1",
  "schema.compatibility": "FULL",
  "transforms": "valueToJson",
  "transforms.valueToJson.fields": "tags,request_headers,request_params,response_headers,request_cookies,response_cookies, api_callee_name_count, tmp-x",
  "transforms.valueToJson.type": "ai.traceable.kafka.connect.smt.RecordFieldToJsonString$Value",
  "fields.whitelist":"customer_id,span_id,span_kind,parent_span_id,trace_id,service_id,api_id,api_name,entry_api_id,protocol_name,backend_id,tags,status_code,start_time_millis,end_time_millis,duration_millis,api_trace_id,service_name,request_headers,request_params,response_headers,request_cookies,response_cookies,request_body,response_body,request_method,api_boundary_type,event_name,user_agent,status_message,status,request_body_type,response_body_type,api_trace_count,display_entity_name,display_span_name,api_has_pii,user_identifier,namespace_name,cluster_name,request_url,http_url_path,api_is_external,error_count,host_header,api_discovery_state,exception_count,user_role,user_scope,user_country,user_country_iso_code,user_state,user_city,user_latitude,user_longitude,ip_address,session_id,bytes_sent,bytes_received,domain_id,environment,api_exit_calls,api_callee_name_count,api_trace_error_span_count,processing_mode,is_bare,is_sampled,spans,sensitive_data_types"
}'