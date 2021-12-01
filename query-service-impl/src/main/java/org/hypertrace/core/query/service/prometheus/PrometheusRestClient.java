package org.hypertrace.core.query.service.prometheus;

import java.io.IOException;
import java.util.Optional;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class PrometheusRestClient {
  private static final String INSTANT_QUERY = "api/v1/query";
  private static final String RANGE_QUERY = "api/v1/query_range";

  private String host;
  private int port;
  private OkHttpClient okHttpClient;

  public PrometheusRestClient(String host, int port, OkHttpClient okHttpClient) {
    this.okHttpClient = okHttpClient;
    this.host = host;
    this.port = port;
  }

  public Optional<PrometheusMetricQueryResponse> executeInstantQuery(PromQLQuery query)
      throws IOException {
    String url = getInstantQueryUrl(query);
    Request request = new Request.Builder().url(url).build();

    try (Response response = okHttpClient.newCall(request).execute()) {
      if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);
      PrometheusMetricQueryResponse data =
          PrometheusMetricQueryResponseParser.parse(response.body().string());
      return Optional.of(data);
    }
  }

  public Optional<PrometheusMetricQueryResponse> executeRangeQuery(PromQLQuery query)
      throws IOException {
    String url = getRangeQueryUrl(query);
    Request request = new Request.Builder().url(url).build();

    try (Response response = okHttpClient.newCall(request).execute()) {
      if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);
      PrometheusMetricQueryResponse data =
          PrometheusMetricQueryResponseParser.parse(response.body().string());
      return Optional.of(data);
    }
  }

  private String getInstantQueryUrl(PromQLQuery query) {
    HttpUrl.Builder urlBuilder = HttpUrl.parse(getApi(INSTANT_QUERY)).newBuilder();
    urlBuilder.addQueryParameter("query", query.getQueries().get(0));
    urlBuilder.addQueryParameter("time", String.valueOf(query.getEndTime().getEpochSecond()));
    return urlBuilder.build().toString();
  }

  private String getRangeQueryUrl(PromQLQuery query) {
    HttpUrl.Builder urlBuilder = HttpUrl.parse(getApi(RANGE_QUERY)).newBuilder();

    urlBuilder.addQueryParameter("query", query.getQueries().get(0));
    urlBuilder.addQueryParameter("start", String.valueOf(query.getStartTime().getEpochSecond()));
    urlBuilder.addQueryParameter("end", String.valueOf(query.getEndTime().getEpochSecond()));
    urlBuilder.addQueryParameter("step", String.valueOf(query.getStep().getSeconds()));

    return urlBuilder.build().toString();
  }

  private String getApi(String path) {
    return String.format("http://%s:%s/%s", this.host, this.port, path);
  }
}
