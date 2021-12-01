package org.hypertrace.core.query.service.prometheus;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
                  public void write(JsonWriter arg0, PrometheusMetricQueryResponse vectorData)
                      throws IOException {}
                })
            .create();
    return gson.fromJson(jsonString, PrometheusMetricQueryResponse.class);
  }

  private static PrometheusMetricQueryResponse parseMetricResponse(JsonReader reader)
      throws IOException {
    List<PrometheusMetricResult> prometheusMetricResultList = new ArrayList<>();
    String status = null;
    String resultType = null;
    reader.beginObject();
    while (reader.hasNext()) {
      String propertyName = reader.nextName();
      if ("status".equals(propertyName)) {
        status = reader.nextString();
      } else if ("data".equals(propertyName)) {
        reader.beginObject();
        while (reader.hasNext()) {
          propertyName = reader.nextName();
          if ("resultType".equals(propertyName)) {
            resultType = reader.nextString();
          } else if ("result".equals(propertyName)) {
            reader.beginArray();
            while (reader.hasNext()) {
              PrometheusMetricResult prometheusMetricResult = convert(reader, resultType);
              prometheusMetricResultList.add(prometheusMetricResult);
            }
            reader.endArray();
          }
        }
        reader.endObject();
      }
    }
    reader.endObject();
    return PrometheusMetricQueryResponse.builder()
        .status(status)
        .resultType(resultType)
        .metrics(prometheusMetricResultList)
        .build();
  }

  private static PrometheusMetricResult convert(JsonReader reader, String resultType)
      throws IOException {
    Map<String, String> metric = new HashMap<String, String>();
    List<PrometheusMetricValue> prometheusMetricValues = new ArrayList<PrometheusMetricValue>();
    reader.beginObject();
    while (reader.hasNext()) {
      String name = reader.nextName();
      if ("metric".equalsIgnoreCase(name)) {
        reader.beginObject();
        while (reader.hasNext()) {
          metric.put(reader.nextName(), reader.nextString());
        }
        reader.endObject();
      } else if ("value".equalsIgnoreCase(name) && resultType.equalsIgnoreCase("vector")) {
        reader.beginArray();
        prometheusMetricValues.add(
            new PrometheusMetricValue((long) reader.nextDouble(), reader.nextDouble()));
        reader.endArray();
      } else if ("values".equalsIgnoreCase(name) && resultType.equalsIgnoreCase("matrix")) {
        reader.beginArray();
        while (reader.hasNext()) {
          reader.beginArray();
          prometheusMetricValues.add(
              new PrometheusMetricValue((long) reader.nextDouble(), reader.nextDouble()));
          reader.endArray();
        }
        reader.endArray();
      }
    }
    reader.endObject();
    return PrometheusMetricResult.builder()
        .metricAttributes(metric)
        .values(prometheusMetricValues)
        .build();
  }
}
