package org.hypertrace.core.query.service;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

public abstract class AbstractServiceTest<TQueryServiceRequestType extends GeneratedMessageV3> {

  private static final String QUERY_SERVICE_TEST_REQUESTS_DIR = "test-requests";

  protected TQueryServiceRequestType readQueryServiceRequest(String fileName) {
    String resourceFileName =
        createResourceFileName(
            QUERY_SERVICE_TEST_REQUESTS_DIR,
            fileName);
    String requestJsonStr = readResourceFileAsString(resourceFileName);

    GeneratedMessageV3.Builder requestBuilder = getQueryServiceRequestBuilder();
    try {
      JsonFormat.parser().merge(requestJsonStr, requestBuilder);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }

    return (TQueryServiceRequestType) requestBuilder.build();
  }

  private static Reader readResourceFile(String fileName) {
    InputStream in = RequestContext.class.getClassLoader().getResourceAsStream(fileName);
    return new BufferedReader(new InputStreamReader(in));
  }

  private String readResourceFileAsString(String fileName) {
    return ((BufferedReader) readResourceFile(fileName)).lines().collect(Collectors.joining());
  }

  private String createResourceFileName(String filesBaseDir, String fileName) {
    return String.format("%s/%s/%s.json", filesBaseDir, getTestSuiteName(), fileName);
  }

  protected abstract String getTestSuiteName();

  protected abstract GeneratedMessageV3.Builder getQueryServiceRequestBuilder();
}
