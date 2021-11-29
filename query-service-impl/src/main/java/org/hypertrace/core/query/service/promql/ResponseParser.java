package org.hypertrace.core.query.service.promql;

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

public class ResponseParser {

  public static MetricResponse parse(String jsonString) {
    Gson gson =
        new GsonBuilder()
            .registerTypeAdapter(
                MetricResponse.class,
                new TypeAdapter<MetricResponse>() {
                  @Override
                  public MetricResponse read(JsonReader reader) throws IOException {
                    return parseMetricResponse(reader);
                  }

                  @Override
                  public void write(JsonWriter arg0, MetricResponse vectorData)
                      throws IOException {}
                })
            .create();
    return gson.fromJson(jsonString, MetricResponse.class);
  }

  private static MetricResponse parseMetricResponse(JsonReader reader) throws IOException {
    List<MetricResult> metricResultList = new ArrayList<>();
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
              MetricResult metricResult = convert(reader, resultType);
              metricResultList.add(metricResult);
            }
            reader.endArray();
          }
        }
        reader.endObject();
      }
    }
    reader.endObject();
    MetricResponse metricResponse = new MetricResponse(status, resultType, metricResultList);
    return metricResponse;
  }

  private static MetricResult convert(JsonReader reader, String resultType) throws IOException {
    Map<String, String> metric = new HashMap<String, String>();
    List<MetricValue> metricValues = new ArrayList<MetricValue>();
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
        metricValues.add(new MetricValue((long)reader.nextDouble(), reader.nextDouble()));
        reader.endArray();
      } else if ("values".equalsIgnoreCase(name) && resultType.equalsIgnoreCase("matrix")) {
        reader.beginArray();
        while (reader.hasNext()) {
          reader.beginArray();
          metricValues.add(new MetricValue((long)reader.nextDouble(), reader.nextDouble()));
          reader.endArray();
        }
        reader.endArray();
      }
    }
    reader.endObject();
    return new MetricResult(metric, metricValues);
  }
}
