package org.hypertrace.core.query.service;

import io.grpc.stub.StreamObserver;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.ResultSetMetadata;
import org.hypertrace.core.query.service.api.Row;

public class ResultSetChunkCollector implements QueryResultCollector<Row> {

  private static final int DEFAULT_CHUNK_SIZE = 10000; // 10k rows
  private final StreamObserver<ResultSetChunk> grpcObserver;
  private final int maxChunkSize;
  private int currentChunkSize;
  private int chunkId;
  private final ResultSetChunk.Builder currentBuilder;

  public ResultSetChunkCollector(StreamObserver<ResultSetChunk> grpcObserver) {
    this(grpcObserver, DEFAULT_CHUNK_SIZE);
  }

  public ResultSetChunkCollector(StreamObserver<ResultSetChunk> grpcObserver, int chunkSize) {
    this.grpcObserver = grpcObserver;
    this.maxChunkSize = chunkSize;
    this.chunkId = 0;
    this.currentBuilder = ResultSetChunk.newBuilder();
    currentBuilder.setChunkId(this.chunkId);
  }

  public void init(ResultSetMetadata metadata) {
    currentBuilder.setResultSetMetadata(metadata);
  }

  public void collect(Row row) {
    currentBuilder.addRow(row);
    currentChunkSize++;
    if (currentChunkSize >= maxChunkSize) {
      ResultSetChunk resultSetChunk = currentBuilder.build();
      grpcObserver.onNext(resultSetChunk);
      currentBuilder.clear();
      chunkId = chunkId + 1;
      currentChunkSize = 0;
      currentBuilder.setChunkId(chunkId);
    }
  }

  public void error(Throwable t) {
    currentBuilder.setIsLastChunk(true);
    currentBuilder.setHasError(true);
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    t.printStackTrace(pw);
    currentBuilder.setErrorMessage(sw.toString());
    grpcObserver.onNext(currentBuilder.build());
  }

  public void finish() {
    // NOTE: Always send a one ResultChunk with isLastChunk = true
    currentBuilder.setIsLastChunk(true);
    ResultSetChunk resultSetChunk = currentBuilder.build();
    grpcObserver.onNext(resultSetChunk);
    grpcObserver.onCompleted();
  }
}
