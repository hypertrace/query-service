package org.hypertrace.core.query.service;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.LoggerFactory;

public class RequestHandlerRegistry {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RequestHandlerRegistry.class);

  Map<String, RequestHandlerInfo> requestHandlerInfoMap = new HashMap<>();

  private static final RequestHandlerRegistry INSTANCE = new RequestHandlerRegistry();

  private RequestHandlerRegistry() {}

  public boolean register(String handlerName, RequestHandlerInfo requestHandlerInfo) {
    if (requestHandlerInfoMap.containsKey(handlerName)) {
      LOG.error("RequestHandlerInfo registration failed. Duplicate Handler:{} ", handlerName);
      return false;
    }
    requestHandlerInfoMap.put(handlerName, requestHandlerInfo);
    return true;
  }

  public Collection<RequestHandlerInfo> getAll() {
    return requestHandlerInfoMap.values();
  }

  public static RequestHandlerRegistry get() {
    return INSTANCE;
  }
}
