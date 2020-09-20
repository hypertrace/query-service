package org.hypertrace.core.query.service;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hypertrace.core.query.service.QueryServiceImplConfig.RequestHandlerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RequestHandlerRegistryTest {

  @Mock QueryServiceImplConfig mockConfig;
  @Mock RequestHandlerBuilder mockBuilder;
  Config testConfig =
      ConfigFactory.parseMap(
          ImmutableMap.<String, Object>builder()
              .put("clientConfig", "testClient")
              .put("name", "testName")
              .put("type", "testType")
              .put("requestHandlerInfo", Map.of())
              .build());

  @BeforeEach
  void beforeEach() {
    when(mockConfig.getQueryRequestHandlersConfig()).thenReturn(List.of(testConfig));
  }

  @Test
  void buildsHandlerIfBuilderFound() {
    RequestHandler mockHandler = mock(RequestHandler.class);
    when(this.mockBuilder.canBuild(any(RequestHandlerConfig.class))).thenReturn(true);
    when(this.mockBuilder.build(any(RequestHandlerConfig.class))).thenReturn(mockHandler);

    assertIterableEquals(
        Set.of(mockHandler), new RequestHandlerRegistry(mockConfig, Set.of(mockBuilder)).getAll());
  }

  @Test
  void testThrowsForUnsupportedHandler() {
    when(this.mockBuilder.canBuild(any(RequestHandlerConfig.class))).thenReturn(false);

    assertThrows(
        UnsupportedOperationException.class,
        () -> new RequestHandlerRegistry(mockConfig, Set.of(mockBuilder)));
  }
}
