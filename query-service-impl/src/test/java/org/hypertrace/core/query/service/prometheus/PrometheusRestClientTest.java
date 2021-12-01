package org.hypertrace.core.query.service.prometheus;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PrometheusRestClientTest {

  @Test
  public void testInstantQuery() throws IOException {
    URL fileUrl =
        PrometheusRestClientTest.class.getClassLoader().getResource("promql_vector_result.json");
    String content = new String(Files.readAllBytes(Paths.get(fileUrl.getFile())));
    String url = "http://localhost?api/v1/query?query=up&time=1435781451";
    OkHttpClient okHttpClient = mockHttpClient(url, content);
    PrometheusRestClient prometheusRestClient =
        new PrometheusRestClient("localhost", 9090, okHttpClient);
    PromQLQuery query =
        PromQLQuery.builder().query("up").evalTimeMs(1435781451000L).isInstantRequest(true).build();
    Optional<PrometheusMetricQueryResponse> metricResponse =
        prometheusRestClient.executeInstantQuery(query);
    Assertions.assertTrue(metricResponse.isPresent());
  }

  @Test
  public void testRangeQuery() throws IOException {
    URL fileUrl =
        PrometheusRestClientTest.class.getClassLoader().getResource("promql_matrix_result.json");
    String content = new String(Files.readAllBytes(Paths.get(fileUrl.getFile())));
    String url = "http://localhost?api/v1/query?query=up&start=1435781430&end=1435781460&step=15";
    OkHttpClient okHttpClient = mockHttpClient(url, content);
    PrometheusRestClient prometheusRestClient =
        new PrometheusRestClient("localhost", 9090, okHttpClient);
    PromQLQuery query =
        PromQLQuery.builder()
            .query("up")
            .startTimeMs(1435781430000L)
            .endTimeMs(1435781460000L)
            .isInstantRequest(false)
            .stepMs(1500)
            .build();

    Optional<PrometheusMetricQueryResponse> metricResponse =
        prometheusRestClient.executeRangeQuery(query);
    Assertions.assertTrue(metricResponse.isPresent());
  }

  private static OkHttpClient mockHttpClient(final String url, final String serializedBody)
      throws IOException {
    final OkHttpClient okHttpClient = mock(OkHttpClient.class);

    final Call remoteCall = mock(Call.class);

    final Response response =
        new Response.Builder()
            .request(new Request.Builder().url(url).build())
            .protocol(Protocol.HTTP_1_1)
            .code(200)
            .message("")
            .body(ResponseBody.create(MediaType.parse("application/json"), serializedBody))
            .build();

    when(remoteCall.execute()).thenReturn(response);
    when(okHttpClient.newCall(any())).thenReturn(remoteCall);

    return okHttpClient;
  }
}
