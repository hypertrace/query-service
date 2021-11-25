package org.hypertrace.core.query.service.promql;

import java.util.ArrayList;
import java.util.List;

public class PromQLQuery {
  private List<String> query;
  private long evalTimeMs;
  private boolean isInstantRequest;
  private long startTimeMs;
  private long endTimeMs;
  private long stepMs;

  private PromQLQuery(Builder builder) {
    this.query = builder.query;
    this.evalTimeMs = builder.evalTimeMs;
    this.isInstantRequest = builder.isInstantRequest;
    this.startTimeMs = builder.startTimeMs;
    this.endTimeMs = builder.endTimeMs;
    this.stepMs = builder.stepMs;
  }

  public static class Builder {
    private List<String> query;
    private long evalTimeMs;
    private boolean isInstantRequest;
    private long startTimeMs;
    private long endTimeMs;
    private long stepMs;

    private Builder() {}

    public static Builder newBuilder() {
      return new Builder();
    }

    public Builder addQuery(String query) {
      if (this.query == null) {
        this.query = new ArrayList<>();
      }
      this.query.add(query);
      return this;
    }

    public Builder setEvalTimeMs(long evalTimeMs) {
      this.evalTimeMs = evalTimeMs;
      return this;
    }

    public Builder setInstantRequest(boolean instantRequest) {
      isInstantRequest = instantRequest;
      return this;
    }

    public Builder setStartTimeMs(long startTimeMs) {
      this.startTimeMs = startTimeMs;
      return this;
    }

    public Builder setEndTimeMs(long endTimeMs) {
      this.endTimeMs = endTimeMs;
      return this;
    }

    public Builder setStepMs(long stepMs) {
      this.stepMs = stepMs;
      return this;
    }

    public PromQLQuery build() {
      return new PromQLQuery(this);
    }
  }

  public List<String> getQuery() {
    return query;
  }

  public long getEvalTimeMs() {
    return evalTimeMs;
  }

  public boolean isInstantRequest() {
    return isInstantRequest;
  }

  public long getStartTimeMs() {
    return startTimeMs;
  }

  public long getEndTimeMs() {
    return endTimeMs;
  }

  public long getStepMs() {
    return stepMs;
  }
}
