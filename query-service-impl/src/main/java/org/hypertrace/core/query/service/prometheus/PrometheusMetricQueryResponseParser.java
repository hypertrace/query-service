package org.hypertrace.core.query.service.prometheus;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;

public class PrometheusMetricQueryResponseParser {

  public static PrometheusMetricQueryResponse parse(String jsonString) {
    Gson gson =
        new GsonBuilder()
            .registerTypeAdapter(
                PrometheusMetricQueryResponse.class,
                new TypeAdapter<PrometheusMetricQueryResponse>() {
                  @Override
                  public PrometheusMetricQueryResponse read(JsonReader reader) throws IOException {
                    return parseMetricResponse(reader);
                  }

                  @Override
                  public void write(JsonWriter writer, PrometheusMetricQueryResponse vectorData)
                      throws IOException {}
                })
            .create();
    return gson.fromJson(jsonString, PrometheusMetricQueryResponse.class);
  }

  private static PrometheusMetricQueryResponse parseMetricResponse(JsonReader reader)
      throws IOException {
    PrometheusMetricQueryResponse.PrometheusMetricQueryResponseBuilder queryResponseBuilder =
        PrometheusMetricQueryResponse.builder();

    // read response object
    reader.beginObject();
    while (reader.hasNext()) {
      String propertyName = reader.nextName();
      if ("status".equals(propertyName)) {
        queryResponseBuilder.status(reader.nextString());
      } else if ("data".equals(propertyName)) {
        parseDataBlock(reader, queryResponseBuilder);
      } else {
        handleErrorBlock(reader, propertyName);
      }
    }
    reader.endObject();

    return queryResponseBuilder.build();
  }

  private static void parseDataBlock(
      JsonReader reader,
      PrometheusMetricQueryResponse.PrometheusMetricQueryResponseBuilder queryResponseBuilder)
      throws IOException {
    String resultType = null;

    // read data block object
    reader.beginObject();
    while (reader.hasNext()) {
      String propertyName = reader.nextName();
      if ("resultType".equals(propertyName)) {
        resultType = reader.nextString();
        queryResponseBuilder.resultType(resultType);
      } else if ("result".equals(propertyName)) {
        parseResultBlock(reader, resultType, queryResponseBuilder);
      }
    }
    reader.endObject();
  }

  private static void parseResultBlock(
      JsonReader reader,
      String resultType,
      PrometheusMetricQueryResponse.PrometheusMetricQueryResponseBuilder queryResponseBuilder)
      throws IOException {
    // read result array
    reader.beginArray();
    while (reader.hasNext()) {
      parseMetricBlock(reader, resultType, queryResponseBuilder);
    }
    reader.endArray();
  }

  private static void parseMetricBlock(
      JsonReader reader,
      String resultType,
      PrometheusMetricQueryResponse.PrometheusMetricQueryResponseBuilder queryResponseBuilder)
      throws IOException {
    PrometheusMetricResult.PrometheusMetricResultBuilder metricResultBuilder =
        PrometheusMetricResult.builder();

    // metric bock is an object
    reader.beginObject();
    while (reader.hasNext()) {
      String name = reader.nextName();
      if ("metric".equalsIgnoreCase(name)) {
        parseMetricAttributes(reader, metricResultBuilder);
      } else if ("value".equalsIgnoreCase(name) && resultType.equalsIgnoreCase("vector")) {
        parseMetricValue(reader, metricResultBuilder);
      } else if ("values".equalsIgnoreCase(name) && resultType.equalsIgnoreCase("matrix")) {
        parseMetricValues(reader, metricResultBuilder);
      }
    }
    reader.endObject();

    queryResponseBuilder.metric(metricResultBuilder.build());
  }

  private static void parseMetricAttributes(
      JsonReader reader, PrometheusMetricResult.PrometheusMetricResultBuilder metricResultBuilder)
      throws IOException {
    // metric attributes is an object
    reader.beginObject();
    while (reader.hasNext()) {
      metricResultBuilder.metricAttribute(reader.nextName(), reader.nextString());
    }
    reader.endObject();
  }

  private static void parseMetricValue(
      JsonReader reader, PrometheusMetricResult.PrometheusMetricResultBuilder metricResultBuilder)
      throws IOException {
    // value is an array of single value
    reader.beginArray();
    metricResultBuilder.value(
        new PrometheusMetricValue(convertTimestamp(reader.nextDouble()), reader.nextDouble()));
    reader.endArray();
  }

  private static void parseMetricValues(
      JsonReader reader, PrometheusMetricResult.PrometheusMetricResultBuilder metricResultBuilder)
      throws IOException {
    // values is an array of array of multiple values
    reader.beginArray();
    while (reader.hasNext()) {
      reader.beginArray();
      metricResultBuilder.value(
          new PrometheusMetricValue(convertTimestamp(reader.nextDouble()), reader.nextDouble()));
      reader.endArray();
    }
    reader.endArray();
  }

  private static void handleErrorBlock(JsonReader reader, String propertyName) throws IOException {
    // handle the error case
    if (!propertyName.equals("warnings")) {
      reader.nextString();
      return;
    }
    reader.beginArray();
    while (reader.hasNext()) {
      reader.nextString();
    }
    reader.endArray();
  }

  private static long convertTimestamp(double timeStamp) {
    return (long) (timeStamp * 1000L);
  }
}
