package org.hypertrace.core.query.service.prometheus;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import okhttp3.Request;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.Row.Builder;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.prometheus.PromQLMetricResponse.PromQLMetricResult;

public class PrometheusBasedResponseBuilder {

  /*
   * @param promQLMetricResponseMap map of prometheus query and its response
   * @param logicalAttributeNameToMetricAttributeMap map of attribute id to prometheus metric attribute name
   * @param logicalAttributeNameToMetricQueryMap map of attribute id of metric to corresponding promQL query
   * @param columnSelectionSet set of all attribute ids of returned column meta data set
   * @param timeStampLogicalAttribute attribute id of time stamp column
   *
   * E.g
   * logicalAttributeNameToMetricAttributeMap :  (SERVICE.id, service_id)
   * logicalAttributeNameToMetricQueryMap (SERVICE.numCalls, sum by (service_id) (sum_over_time(num_calls{}[]))
   * columnSelectionSet : [SERVICE.startTime, SERVICE.id, SERVICE.numCall]
   * timeStampLogicalAttribute : SERVICE.startTime
   * */
  static List<Row> buildResponse(
      Map<Request, PromQLMetricResponse> promQLMetricResponseMap,
      Map<String, String> logicalAttributeNameToMetricAttributeMap,
      Map<String, String> logicalAttributeNameToMetricQueryMap,
      List<String> columnSelectionSet,
      String timeStampLogicalAttribute) {

    // check if response is empty
    if (promQLMetricResponseMap.isEmpty()) {
      return List.of();
    }

    // convert <request, response> map to <query, response> map
    Map<String, PromQLMetricResponse> metricResponseMap =
        promQLMetricResponseMap.entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> entry.getKey().url().queryParameter("query"), Entry::getValue));

    // as multiple request only vary in metric value, and all other attributes
    // and number of rows are same, we can use one of the metric response for
    // building attribute columns, for metric columns we can use other response
    PromQLMetricResponse firstResponse =
        promQLMetricResponseMap.values().stream().findFirst().get();
    List<Builder> builderList =
        buildAggregateResponse(
            metricResponseMap,
            logicalAttributeNameToMetricAttributeMap,
            logicalAttributeNameToMetricQueryMap,
            columnSelectionSet,
            firstResponse,
            timeStampLogicalAttribute);

    return builderList.stream().map(Builder::build).collect(Collectors.toList());
  }

  private static List<Builder> buildAggregateResponse(
      Map<String, PromQLMetricResponse> promQLMetricResponseMap,
      Map<String, String> logicalAttributeNameToMetricAttributeMap,
      Map<String, String> logicalAttributeNameToMetricQueryMap,
      List<String> columnSelectionSet,
      PromQLMetricResponse firstResponse,
      String timeStampLogicalAttribute) {

    List<Builder> rowBuilderList = new ArrayList<>();

    // number of rows as per grouping eg. group by (service_id, service_name)
    int numGroupBasedRows = firstResponse.getData().getResult().size();
    for (int groupBasedRow = 0; groupBasedRow < numGroupBasedRows; groupBasedRow++) {
      PromQLMetricResult firstResponseMetricResult =
          firstResponse.getData().getResult().get(groupBasedRow);
      // number of rows as per time series values
      // For instant query, it will be a single time
      // FoR Range query, it will be multiple times as per the step (or duration)
      int numTimeBasedRows = firstResponseMetricResult.getValues().size();
      for (int timeBasedRow = 0; timeBasedRow < numTimeBasedRows; timeBasedRow++) {
        Builder rowBuilder = Row.newBuilder();
        Instant rowTimeStamp =
            firstResponseMetricResult.getValues().get(timeBasedRow).getTimeStamp();
        // column loop : prepare each column of the row
        columnSelectionSet.forEach(
            selection -> {
              if (logicalAttributeNameToMetricQueryMap.containsKey(selection)) {
                // metric selection
                String colVal =
                    getMetricValueForTime(
                        logicalAttributeNameToMetricQueryMap.get(selection),
                        promQLMetricResponseMap,
                        firstResponseMetricResult,
                        rowTimeStamp);
                rowBuilder.addColumn(Value.newBuilder().setString(colVal).build());
              } else if (logicalAttributeNameToMetricAttributeMap.containsKey(selection)
                  && !timeStampLogicalAttribute.equals(selection)) {
                // attribute selection other than timestamp attribute
                String colVal =
                    getMetricAttributeValue(
                        logicalAttributeNameToMetricAttributeMap.get(selection),
                        firstResponseMetricResult);
                rowBuilder.addColumn(Value.newBuilder().setString(colVal).build());
              } else if (timeStampLogicalAttribute.equals(selection)) {
                // time stamp attribute
                String colVal = String.valueOf(rowTimeStamp.toEpochMilli());
                rowBuilder.addColumn(Value.newBuilder().setString(colVal).build());
              } else {
                throw new RuntimeException(
                    String.format(
                        "Mapping of selection:{%s} in metric attributeMap and in query map is not found",
                        selection));
              }
            });

        rowBuilderList.add(rowBuilder);
      }
    }
    return rowBuilderList;
  }

  private static Map<String, String> getMetricAttributesWithoutMetricName(
      Map<String, String> metricAttributes) {
    return metricAttributes.entrySet().stream()
        .filter(entry -> !entry.getKey().equals("__name__"))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static String getMetricAttributeValue(
      String selection, PromQLMetricResult promQLMetricResult) {
    return promQLMetricResult.getMetricAttributes().get(selection);
  }

  private static String getMetricValueForTime(
      String promQlQuery,
      Map<String, PromQLMetricResponse> promQLMetricResponseMap,
      PromQLMetricResult inputMetricResult,
      Instant rowTimeStamp) {

    PromQLMetricResponse promQLMetricResponse = promQLMetricResponseMap.get(promQlQuery);

    return String.valueOf(
        promQLMetricResponse.getData().getResult().stream()
            .filter(
                metricResult ->
                    getMetricAttributesWithoutMetricName(metricResult.getMetricAttributes())
                        .equals(
                            getMetricAttributesWithoutMetricName(
                                inputMetricResult.getMetricAttributes())))
            .map(
                matchedResult ->
                    matchedResult.getValues().stream()
                        .filter(value -> value.getTimeStamp().equals(rowTimeStamp))
                        .findFirst()
                        .orElseThrow()
                        .getValue())
            .findFirst()
            .orElseThrow());
  }
}
