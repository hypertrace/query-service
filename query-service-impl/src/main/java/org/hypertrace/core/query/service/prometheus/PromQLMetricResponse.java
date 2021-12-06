package org.hypertrace.core.query.service.prometheus;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Singular;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
class PromQLMetricResponse {
  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty("status")
  private String status;

  @JsonProperty("data")
  private PromQLData data;

  @Getter
  static class PromQLData {
    @JsonProperty("resultType")
    private String resultType;

    @JsonProperty("result")
    private List<PromQLMetricResult> result;
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @EqualsAndHashCode
  static class PromQLMetricResult {
    @JsonProperty("metric")
    @Singular
    private Map<String, String> metricAttributes;

    @JsonAlias({"value", "values"})
    @JsonDeserialize(using = PromQLMetricValuesDeserializer.class)
    @Singular
    private List<PromQLMetricValue> values;
  }

  @AllArgsConstructor
  @Getter
  @EqualsAndHashCode
  static class PromQLMetricValue {
    private long timeStamp;
    private double value;
  }

  static class PromQLMetricValuesDeserializer extends JsonDeserializer<List<PromQLMetricValue>> {

    @Override
    public List<PromQLMetricValue> deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      List<PromQLMetricValue> metricValues = new ArrayList<>();
      JsonNode node = parser.getCodec().readTree(parser);
      if (node.isArray() && node.get(0).isArray()) {
        node.elements().forEachRemaining(e -> metricValues.add(parseValue((ArrayNode) e)));
      } else {
        metricValues.add(parseValue((ArrayNode) node));
      }
      return metricValues;
    }

    private PromQLMetricValue parseValue(ArrayNode arrayNode) {
      long timestamp = (long) arrayNode.get(0).asDouble() * 1000L;
      double value = arrayNode.get(1).asDouble();
      return new PromQLMetricValue(timestamp, value);
    }
  }

  static PromQLMetricResponse fromJson(String json) throws IOException {
    return OBJECT_MAPPER.readValue(json, PromQLMetricResponse.class);
  }
}
