package org.hypertrace.core.query.service.promql;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class PromQLRestClient {
  private static final String INSTANT_QUERY = "/api/v1/query";
  private static final String RANGE_QUERY = "/api/v1/query_range";

  private String endPoint;
  private OkHttpClient okHttpClient;

  public PromQLRestClient(String endPoint, OkHttpClient okHttpClient) {
    this.okHttpClient = okHttpClient;
    this.endPoint = endPoint;
  }

  public Optional<MetricResponse> executeInstantQuery(PromQLQuery query) throws IOException {
    String url = getInstantQueryUrl(query);
    Request request = new Request.Builder().url(url).build();

    try (Response response = okHttpClient.newCall(request).execute()) {
      if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);
      MetricResponse data = ResponseParser.parse(response.body().string());
      return Optional.of(data);
    }
  }

  public String getInstantQueryUrl(PromQLQuery query) {
    HttpUrl.Builder urlBuilder = HttpUrl.parse("http://" + endPoint + INSTANT_QUERY).newBuilder();
    Instant evalTime = Instant.ofEpochMilli(query.getEvalTimeMs());
    urlBuilder.addQueryParameter("query", query.getQueries().get(0));
    urlBuilder.addQueryParameter("time", String.valueOf(evalTime.getEpochSecond()));
    return urlBuilder.build().toString();
  }

  public Optional<MetricResponse> executeRangeQuery(PromQLQuery query) throws IOException {
    String url = getRangeQueryUrl(query);
    Request request = new Request.Builder().url(url).build();

    try (Response response = okHttpClient.newCall(request).execute()) {
      if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);
      MetricResponse data = ResponseParser.parse(response.body().string());
      return Optional.of(data);
    }
  }

  public String getRangeQueryUrl(PromQLQuery query) {
    HttpUrl.Builder urlBuilder = HttpUrl.parse("http://" + endPoint + INSTANT_QUERY).newBuilder();
    Instant startTime = Instant.ofEpochMilli(query.getStartTimeMs());
    Instant endTime = Instant.ofEpochMilli(query.getEndTimeMs());
    Duration duration = Duration.of(query.getStepMs(), ChronoUnit.MILLIS);

    urlBuilder.addQueryParameter("query", query.getQueries().get(0));
    urlBuilder.addQueryParameter("start", String.valueOf(startTime.getEpochSecond()));
    urlBuilder.addQueryParameter("end", String.valueOf(endTime.getEpochSecond()));
    urlBuilder.addQueryParameter("step", String.valueOf(duration.getSeconds()));

    return urlBuilder.build().toString();
  }
}
