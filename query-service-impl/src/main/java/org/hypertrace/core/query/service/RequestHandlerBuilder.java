package org.hypertrace.core.query.service;

import org.hypertrace.core.query.service.QueryServiceImplConfig.RequestHandlerConfig;

public interface RequestHandlerBuilder {

  boolean canBuild(RequestHandlerConfig config);

  RequestHandler build(RequestHandlerConfig config);
}
