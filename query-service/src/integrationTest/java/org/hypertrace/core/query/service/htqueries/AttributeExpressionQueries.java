package org.hypertrace.core.query.service.htqueries;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import org.apache.kafka.common.requests.RequestContext;
import org.hypertrace.core.query.service.api.QueryRequest;

class AttributeExpressionQueries {

  private static Reader readResourceFile(String fileName) {
    InputStream in = RequestContext.class.getClassLoader().getResourceAsStream(fileName);
    return new BufferedReader(new InputStreamReader(in));
  }

  // format for new tests
  static QueryRequest buildQuery1() throws IOException {
    String resourceFileName = "attribute-expression-test-queries/query1.json";
    Reader requestJsonStr = readResourceFile(resourceFileName);
    QueryRequest.Builder builder = QueryRequest.newBuilder();
    try {
      JsonFormat.parser().merge(requestJsonStr, builder);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }
    return builder.build();
  }
}
