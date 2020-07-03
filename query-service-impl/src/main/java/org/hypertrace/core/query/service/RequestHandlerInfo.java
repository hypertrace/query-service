package org.hypertrace.core.query.service;

import java.util.Map;

public class RequestHandlerInfo {

  private String name;

  private Class<? extends RequestHandler> requestHandlerClazz;

  // todo:change to concrete class later
  private Map<String, Object> config;

  public RequestHandlerInfo(
      String name,
      Class<? extends RequestHandler> requestHandlerClazz,
      Map<String, Object> config) {
    this.name = name;
    this.requestHandlerClazz = requestHandlerClazz;
    this.config = config;
  }

  public String getName() {
    return name;
  }

  public Class<? extends RequestHandler> getRequestHandlerClazz() {
    return requestHandlerClazz;
  }

  public Map<String, Object> getConfig() {
    return config;
  }
}
