package org.hypertrace.core.query.service.prometheus;

import java.time.Instant;
import java.util.ArrayList;
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
   * PromQL response
   * Map<columnName, metrics_attribute> (SERVICE.id, service_id)
   * Map<columnName, query_name> (SERVICE.numCalls, query) -> merticMap
   * columnSet : list of all selected columns request metatdata
   * <SERVICE.startTime, SERVICE.id, SERVICE.numCall, Service.errorCount> // columnSet
   * timeStampColumn : SERVICE.startTime
   * */
  static List<Row> buildResponse(
      Map<Request, PromQLMetricResponse> promQLMetricResponseMap,
      Map<String, String> columnNameToAttributeMap,
      Map<String, String> columnNameToQueryMap,
      List<String> columnSet,
      String timeStampColumn) {

    // check if response is empty
    if (promQLMetricResponseMap.isEmpty()) {
      return null;
    }

    // as multiple request only vary in metric value, and all other attributes
    // and number of rows are same, we can use one of the metric response for
    // building attribute columns, for metric columns we can use other response
    PromQLMetricResponse firstResponse =
        promQLMetricResponseMap.values().stream().findFirst().get();

    List<Builder> builderList;
    if (isInstantResponse(firstResponse)) {
      builderList =
          buildAggregateResponse(
              promQLMetricResponseMap,
              columnNameToAttributeMap,
              columnNameToQueryMap,
              columnSet,
              firstResponse);
    } else {
      builderList =
          buildAggregateResponse(
              promQLMetricResponseMap,
              columnNameToAttributeMap,
              columnNameToQueryMap,
              columnSet,
              firstResponse,
              timeStampColumn);
    }

    return builderList.stream().map(builder -> builder.build()).collect(Collectors.toList());
  }

  private static List<Builder> buildAggregateResponse(
      Map<Request, PromQLMetricResponse> promQLMetricResponseMap,
      Map<String, String> columnNameToAttributeMap,
      Map<String, String> columnNameToQueryMap,
      List<String> columnSet,
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
              } else if (columnNameToAttributeMap.containsKey(selection)) {
                // attribute selection
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
      Map<Request, PromQLMetricResponse> promQLMetricResponseMap,
      Map<String, String> columnNameToAttributeMap,
      Map<String, String> columnNameToQueryMap,
      List<String> columnSet,
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
              // attribute selection
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

  private static String getMetricValue(
      String query,
      Map<Request, PromQLMetricResponse> promQLMetricResponseMap,
      PromQLMetricResult promQLMetricResult) {
    PromQLMetricResponse matchedResponse =
        promQLMetricResponseMap.entrySet().stream()
            .filter(entry -> entry.getKey().url().queryParameter("query").equals(query))
            .findFirst()
            .get()
            .getValue();

    return String.valueOf(
        matchedResponse.getData().getResult().stream()
            .filter(
                metricResult ->
                    metricResult
                        .getMetricAttributes()
                        .equals(promQLMetricResult.getMetricAttributes()))
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
      Map<Request, PromQLMetricResponse> promQLMetricResponseMap,
      PromQLMetricResult promQLMetricResult,
      Instant time) {
    PromQLMetricResponse matchedResponse =
        promQLMetricResponseMap.entrySet().stream()
            .filter(entry -> entry.getKey().url().queryParameter("query").equals(query))
            .findFirst()
            .get()
            .getValue();

    return String.valueOf(
        matchedResponse.getData().getResult().stream()
            .filter(
                metricResult ->
                    metricResult
                        .getMetricAttributes()
                        .equals(promQLMetricResult.getMetricAttributes()))
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
