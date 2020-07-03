package org.hypertrace.core.query.service.pinot;

import java.util.HashMap;
import java.util.Map;

/**
 * Holds the params that need to be set in the PreparedStatement for constructing the final PQL
 * query
 */
public class Params {

  // Map of index to the corresponding param value
  private Map<Integer, Integer> integerParams;
  private Map<Integer, Long> longParams;
  private Map<Integer, String> stringParams;
  private Map<Integer, Float> floatParams;
  private Map<Integer, Double> doubleParams;

  private Params(
      Map<Integer, Integer> integerParams,
      Map<Integer, Long> longParams,
      Map<Integer, String> stringParams,
      Map<Integer, Float> floatParams,
      Map<Integer, Double> doubleParams) {
    this.integerParams = integerParams;
    this.longParams = longParams;
    this.stringParams = stringParams;
    this.floatParams = floatParams;
    this.doubleParams = doubleParams;
  }

  public Map<Integer, Integer> getIntegerParams() {
    return integerParams;
  }

  public Map<Integer, Long> getLongParams() {
    return longParams;
  }

  public Map<Integer, String> getStringParams() {
    return stringParams;
  }

  public Map<Integer, Float> getFloatParams() {
    return floatParams;
  }

  public Map<Integer, Double> getDoubleParams() {
    return doubleParams;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private int nextIndex;
    private Map<Integer, Integer> integerParams;
    private Map<Integer, Long> longParams;
    private Map<Integer, String> stringParams;
    private Map<Integer, Float> floatParams;
    private Map<Integer, Double> doubleParams;

    private Builder() {
      nextIndex = 0;
      integerParams = new HashMap<>();
      longParams = new HashMap<>();
      stringParams = new HashMap<>();
      floatParams = new HashMap<>();
      doubleParams = new HashMap<>();
    }

    public Builder addIntegerParam(int paramValue) {
      integerParams.put(nextIndex++, paramValue);
      return this;
    }

    public Builder addLongParam(long paramValue) {
      longParams.put(nextIndex++, paramValue);
      return this;
    }

    public Builder addStringParam(String paramValue) {
      stringParams.put(nextIndex++, paramValue);
      return this;
    }

    public Builder addFloatParam(float paramValue) {
      floatParams.put(nextIndex++, paramValue);
      return this;
    }

    public Builder addDoubleParam(double paramValue) {
      doubleParams.put(nextIndex++, paramValue);
      return this;
    }

    public Params build() {
      return new Params(integerParams, longParams, stringParams, floatParams, doubleParams);
    }
  }
}
