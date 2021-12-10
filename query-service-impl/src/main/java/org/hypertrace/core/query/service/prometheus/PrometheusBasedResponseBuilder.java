package org.hypertrace.core.query.service.prometheus;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import okhttp3.Request;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.Row.Builder;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.prometheus.PromQLMetricResponse.PromQLMetricResult;

public class PrometheusBasedResponseBuilder {

  /*
   * @param promQLMetricResponseMap map of prometheus query and its response
   * @param columnNameToAttributeMap map of attribute id to prometheus metric attribute name
   * @param columnNameToQueryMap map of attribute id of metric to corresponding promQL query
   * @param columnSet set of all attribute ids of returned column meta data set
   * @param timeStampColumn attribute id of time stamp column
   *
   * E.g
   * columnNameToAttributeMap :  (SERVICE.id, service_id)
   * columnNameToQueryMap (SERVICE.numCalls, sum by (service_id) (sum_over_time(num_calls{}[]))
   * columnSet : [SERVICE.startTime, SERVICE.id, SERVICE.numCall]
   * timeStampColumn : SERVICE.startTime
   * */
  static List<Row> buildResponse(
      Map<Request, PromQLMetricResponse> promQLMetricResponseMap,
      Map<String, String> columnNameToAttributeMap,
      Map<String, String> columnNameToQueryMap,
      LinkedHashSet<String> columnSet,
      String timeStampColumn) {

    // check if response is empty
    if (promQLMetricResponseMap.isEmpty()) {
      return List.of();
    }

    // convert map to query -> response
    Map<String, PromQLMetricResponse> metricResponseMap =
        promQLMetricResponseMap.entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> entry.getKey().url().queryParameter("query"),
                    entry -> entry.getValue()));

    List<Builder> builderList = new ArrayList<>();

    // as multiple request only vary in metric value, and all other attributes
    // and number of rows are same, we can use one of the metric response for
    // building attribute columns, for metric columns we can use other response
    PromQLMetricResponse firstResponse =
        promQLMetricResponseMap.values().stream().findFirst().get();

    if (isInstantResponse(firstResponse)) {
      builderList =
          buildAggregateResponse(
              metricResponseMap,
              columnNameToAttributeMap,
              columnNameToQueryMap,
              columnSet,
              firstResponse);
    } else if (isRangeResponse(firstResponse)) {
      builderList =
          buildAggregateResponse(
              metricResponseMap,
              columnNameToAttributeMap,
              columnNameToQueryMap,
              columnSet,
              firstResponse,
              timeStampColumn);
    }

    return builderList.stream().map(builder -> builder.build()).collect(Collectors.toList());
  }

  private static List<Builder> buildAggregateResponse(
      Map<String, PromQLMetricResponse> promQLMetricResponseMap,
      Map<String, String> columnNameToAttributeMap,
      Map<String, String> columnNameToQueryMap,
      LinkedHashSet<String> columnSet,
      PromQLMetricResponse firstResponse,
      String timeStampColumn) {

    List<Builder> rowBuilderList = new ArrayList<>();
    int numRows = (int) calcNumberRows(firstResponse);
    for (int row = 0; row < numRows; row++) {
      PromQLMetricResult promQLMetricResult = firstResponse.getData().getResult().get(row);
      // now time loop
      int numTimeRows = promQLMetricResult.getValues().size();
      for (int timeRow = 0; timeRow < numTimeRows; timeRow++) {
        Builder rowBuilder = Row.newBuilder();
        Instant time = promQLMetricResult.getValues().get(timeRow).getTimeStamp();
        // column loop
        columnSet.forEach(
            selection -> {
              if (columnNameToQueryMap.containsKey(selection)) {
                // metric selection
                String colVal =
                    getMetricValueForTime(
                        columnNameToQueryMap.get(selection),
                        promQLMetricResponseMap,
                        promQLMetricResult,
                        time);
                rowBuilder.addColumn(Value.newBuilder().setString(colVal).build());
              } else if (columnNameToAttributeMap.containsKey(selection)
                  && !timeStampColumn.equals(selection)) {
                // attribute selection (note: should have only supported metric attributes)
                String colVal =
                    getMetricAttributeValue(
                        columnNameToAttributeMap.get(selection), promQLMetricResult);
                rowBuilder.addColumn(Value.newBuilder().setString(colVal).build());
              } else if (timeStampColumn.equals(selection)) {
                // time stamp attribute
                String colVal = String.valueOf(time.toEpochMilli());
                rowBuilder.addColumn(Value.newBuilder().setString(colVal).build());
              } else {
                throw new RuntimeException("Invalid selection");
              }
            });

        rowBuilderList.add(rowBuilder);
      }
    }

    return rowBuilderList;
  }

  private static List<Builder> buildAggregateResponse(
      Map<String, PromQLMetricResponse> promQLMetricResponseMap,
      Map<String, String> columnNameToAttributeMap,
      Map<String, String> columnNameToQueryMap,
      LinkedHashSet<String> columnSet,
      PromQLMetricResponse firstResponse) {

    List<Builder> rowBuilderList = new ArrayList<>();
    int numRows = (int) calcNumberRows(firstResponse);
    for (int row = 0; row < numRows; row++) {
      Builder rowBuilder = Row.newBuilder();
      PromQLMetricResult promQLMetricResult = firstResponse.getData().getResult().get(row);

      // iterate over column and prepare a row
      columnSet.forEach(
          selection -> {
            if (columnNameToQueryMap.containsKey(selection)) {
              // metric selection
              String colVal =
                  getMetricValue(
                      columnNameToQueryMap.get(selection),
                      promQLMetricResponseMap,
                      promQLMetricResult);
              rowBuilder.addColumn(Value.newBuilder().setString(colVal).build());
            } else if (columnNameToAttributeMap.containsKey(selection)) {
              // attribute selection (note: should have only supported metric attributes)
              String colVal =
                  getMetricAttributeValue(
                      columnNameToAttributeMap.get(selection), promQLMetricResult);
              rowBuilder.addColumn(Value.newBuilder().setString(colVal).build());
            } else {
              throw new RuntimeException("Invalid selection");
            }
          });

      rowBuilderList.add(rowBuilder);
    }
    return rowBuilderList;
  }

  private static boolean isInstantResponse(PromQLMetricResponse firstResponse) {
    return firstResponse.getData().getResultType().equals("vector");
  }

  private static boolean isRangeResponse(PromQLMetricResponse firstResponse) {
    return firstResponse.getData().getResultType().equals("matrix");
  }

  private static long calcNumberRows(PromQLMetricResponse firstResponse) {
    return firstResponse.getData().getResult().size();
  }

  private static Map<String, String> getWithoutMetricName(Map<String, String> metricAttributes) {
    return metricAttributes.entrySet().stream()
        .filter(entry -> !entry.getKey().equals("__name__"))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static String getMetricValue(
      String query,
      Map<String, PromQLMetricResponse> promQLMetricResponseMap,
      PromQLMetricResult promQLMetricResult) {

    PromQLMetricResponse matchedResponse = promQLMetricResponseMap.get(query);

    return String.valueOf(
        matchedResponse.getData().getResult().stream()
            .filter(
                metricResult ->
                    getWithoutMetricName(metricResult.getMetricAttributes())
                        .equals(getWithoutMetricName(promQLMetricResult.getMetricAttributes())))
            .map(result -> result.getValues().get(0).getValue())
            .findFirst()
            .get());
  }

  private static String getMetricAttributeValue(
      String selection, PromQLMetricResult promQLMetricResult) {
    return promQLMetricResult.getMetricAttributes().get(selection);
  }

  private static String getMetricValueForTime(
      String query,
      Map<String, PromQLMetricResponse> promQLMetricResponseMap,
      PromQLMetricResult promQLMetricResult,
      Instant time) {

    PromQLMetricResponse matchedResponse = promQLMetricResponseMap.get(query);

    return String.valueOf(
        matchedResponse.getData().getResult().stream()
            .filter(
                metricResult ->
                    getWithoutMetricName(metricResult.getMetricAttributes())
                        .equals(getWithoutMetricName(promQLMetricResult.getMetricAttributes())))
            .map(
                matchedResult ->
                    matchedResult.getValues().stream()
                        .filter(value -> value.getTimeStamp().equals(time))
                        .findFirst()
                        .get()
                        .getValue())
            .findFirst()
            .get());
  }
}
