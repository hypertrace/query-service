package org.hypertrace.core.query.service.prometheus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import okhttp3.Request;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.Row.Builder;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.prometheus.PromQLMetricResponse.PromQLMetricResult;

public class PrometheusBasedResponseBuilder {

  /*
   * PromQL response
   * Map<columnName, metrics_attribute> (SERVICE.id, service_id)
   * Map<columnName, query_name> (SERVICE.numCalls, query)
   * selectionList : list of all selected columns
   * */
  public static void buildQSResponse(
      Map<Request, PromQLMetricResponse> promQLMetricResponseMap,
      Map<String, String> columnNameToAttributeMap,
      Map<String, String> columnNameToQueryMap,
      List<String> selectionList) {

    if (promQLMetricResponseMap.isEmpty()) {
      return;
    }

    PromQLMetricResponse firstResponse =
        promQLMetricResponseMap.values().stream().findFirst().get();

    if (checkIfVectorResponse(firstResponse)) {
      buildAggregateResponse(
          promQLMetricResponseMap,
          columnNameToAttributeMap,
          columnNameToQueryMap,
          selectionList,
          firstResponse);
    } else {

    }
  }

  private static List<Builder> buildAggregateResponse(
      Map<Request, PromQLMetricResponse> promQLMetricResponseMap,
      Map<String, String> columnNameToAttributeMap,
      Map<String, String> columnNameToQueryMap,
      List<String> selectionList,
      PromQLMetricResponse firstResponse) {
    List<Builder> rowBuilderList = new ArrayList<>();
    for (int row = 0; row < calcNumberRows(firstResponse); row++) {
      Builder rowBuilder = Row.newBuilder();

      PromQLMetricResult promQLMetricResult = firstResponse.getData().getResult().get(row);

      // iterate over column and prepare a row
      selectionList.forEach(
          selection -> {
            // is metric selection
            if (columnNameToQueryMap.containsKey(selection)) {
              // yes its metric value
              String colVal =
                  getMetricValue(
                      columnNameToQueryMap.get(selection),
                      promQLMetricResponseMap,
                      promQLMetricResult);
              rowBuilder.addColumn(Value.newBuilder().setString(colVal).build());
            } else if (columnNameToAttributeMap.containsKey(selection)) {
              // yes, its attribute value
              String colVal = getMetricAttributeValue(selection, promQLMetricResult);
              rowBuilder.addColumn(Value.newBuilder().setString(colVal).build());
            } else {
              throw new RuntimeException("Invalid selection");
            }
          });

      rowBuilderList.add(rowBuilder);
    }
    return rowBuilderList;
  }

  private static boolean checkIfVectorResponse(PromQLMetricResponse firstResponse) {
    return firstResponse.getData().getResultType().equals("vector");
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
            .map(result -> result.getValues().get(0).getValue()));
  }

  private static String getMetricAttributeValue(
      String selection, PromQLMetricResult promQLMetricResult) {
    return promQLMetricResult.getMetricAttributes().get(selection);
  }
}
