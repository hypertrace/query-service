package org.hypertrace.core.query.service.prometheus;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.NoArgsConstructor;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.lang3.tuple.ImmutablePair;

class PrometheusRestClient {
  private static final String INSTANT_QUERY = "api/v1/query";
  private static final String RANGE_QUERY = "api/v1/query_range";

  private final String host;
  private final int port;
  private final OkHttpClient okHttpClient;

  PrometheusRestClient(String host, int port) {
    this.okHttpClient = new OkHttpClient();
    this.host = host;
    this.port = port;
  }

  public Map<Request, PromQLMetricResponse> executeInstantQuery(PromQLInstantQueries instantQuery) {
    List<Request> requests = getInstantQueryRequests(instantQuery);
    return execute(requests);
  }

  public Map<Request, PromQLMetricResponse> executeRangeQuery(PromQLRangeQueries rangeQuery) {
    List<Request> requests = getRangeQueryRequests(rangeQuery);
    return execute(requests);
  }

  private Map<Request, PromQLMetricResponse> execute(List<Request> requests) {
    List<OkHttpResponseCallback> okHttpResponseCallbacks =
        requests.stream()
            .map(
                request -> {
                  Call call = okHttpClient.newCall(request);
                  OkHttpResponseCallback callback = new OkHttpResponseCallback(request);
                  call.enqueue(callback);
                  return callback;
                })
            .collect(Collectors.toUnmodifiableList());

    CompletableFuture.allOf(
            okHttpResponseCallbacks.stream()
                .map(okHttpResponseCallback -> okHttpResponseCallback.future)
                .toArray(CompletableFuture[]::new))
        .join();

    return okHttpResponseCallbacks.stream()
        .map(
            okHttpResponseCallback ->
                ImmutablePair.of(
                    okHttpResponseCallback.request,
                    convertResponse(okHttpResponseCallback.future.join())))
        .collect(Collectors.toMap(pair -> pair.getLeft(), pair -> pair.getRight()));
  }

  private PromQLMetricResponse convertResponse(Response response) {
    try {
      if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);
      return PromQLMetricResponse.fromJson(response.body().string());
    } catch (IOException ioException) {
      throw new RuntimeException(ioException);
    }
  }

  private List<Request> getInstantQueryRequests(PromQLInstantQueries promQLQuery) {
    return promQLQuery.getQueries().stream()
        .map(
            query -> {
              HttpUrl.Builder urlBuilder = HttpUrl.parse(getRequestUrl(INSTANT_QUERY)).newBuilder();
              urlBuilder.addQueryParameter("query", query);
              urlBuilder.addQueryParameter(
                  "time", String.valueOf(promQLQuery.getEvalTime().getEpochSecond()));
              return new Request.Builder().url(urlBuilder.build().toString()).build();
            })
        .collect(Collectors.toList());
  }

  private List<Request> getRangeQueryRequests(PromQLRangeQueries promQLQuery) {
    return promQLQuery.getQueries().stream()
        .map(
            query -> {
              HttpUrl.Builder urlBuilder = HttpUrl.parse(getRequestUrl(RANGE_QUERY)).newBuilder();
              urlBuilder.addQueryParameter("query", query);
              urlBuilder.addQueryParameter(
                  "start", String.valueOf(promQLQuery.getStartTime().getEpochSecond()));
              urlBuilder.addQueryParameter(
                  "end", String.valueOf(promQLQuery.getEndTime().getEpochSecond()));
              urlBuilder.addQueryParameter(
                  "step", String.valueOf(promQLQuery.getPeriod().getSeconds()));
              return new Request.Builder().url(urlBuilder.build().toString()).build();
            })
        .collect(Collectors.toList());
  }

  private String getRequestUrl(String path) {
    return String.format("http://%s:%s/%s", this.host, this.port, path);
  }

  @NoArgsConstructor
  private static class OkHttpResponseCallback implements Callback {
    private final CompletableFuture<Response> future = new CompletableFuture<>();
    private Request request;

    public OkHttpResponseCallback(Request request) {
      this.request = request;
    }

    @Override
    public void onResponse(Call call, Response response) throws IOException {
      future.complete(response);
    }

    @Override
    public void onFailure(Call call, IOException e) {
      future.completeExceptionally(e);
    }
  }
}
