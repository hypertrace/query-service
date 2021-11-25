package org.hypertrace.core.query.service.promql;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class PromQLRestClient {
  private static final String INSTANT_QUERY = "/api/v1/query";
  private static final String RANGE_QUERY = "/api/v1/query_range";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String endPoint;
  private OkHttpClient client;

  public PromQLRestClient(String endPoint) {
    client = new OkHttpClient();
    this.endPoint = endPoint;
  }

  public void executeInstantQuery(PromQLQuery query) throws IOException {
    HttpUrl.Builder urlBuilder = HttpUrl.parse(endPoint + INSTANT_QUERY).newBuilder();
    urlBuilder.addQueryParameter("query", query.getQuery().get(0));
    urlBuilder.addQueryParameter("time", String.valueOf(query.getEvalTimeMs()));

    String url = urlBuilder.build().toString();

    Request request = new Request.Builder().url(url).build();

    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);
      System.out.println(response.body().string());
    }
  }

  public void executeRangeQuery(PromQLQuery query) throws IOException {
    HttpUrl.Builder urlBuilder = HttpUrl.parse(endPoint + INSTANT_QUERY).newBuilder();
    urlBuilder.addQueryParameter("query", query.getQuery().get(0));
    urlBuilder.addQueryParameter("start", String.valueOf(query.getStartTimeMs()));
    urlBuilder.addQueryParameter("end", String.valueOf(query.getEndTimeMs()));
    urlBuilder.addQueryParameter("step", String.valueOf(query.getStepMs()));

    String url = urlBuilder.build().toString();

    Request request = new Request.Builder().url(url).build();

    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);
      System.out.println(response.body().string());
    }
  }
}
