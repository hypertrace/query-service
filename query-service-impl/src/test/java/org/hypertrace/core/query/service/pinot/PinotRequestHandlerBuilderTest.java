package org.hypertrace.core.query.service.pinot;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.hypertrace.core.query.service.QueryServiceImplConfig.RequestHandlerConfig;
import org.hypertrace.core.query.service.RequestClientConfigRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PinotRequestHandlerBuilderTest {

  @Test
  void testThrowsIfNoMatchingConfig() {
    RequestHandlerConfig mockConfig = mock(RequestHandlerConfig.class);
    RequestClientConfigRegistry mockConfigRegistry = mock(RequestClientConfigRegistry.class);
    when(mockConfigRegistry.get(any())).thenReturn(Optional.empty());

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> new PinotRequestHandlerBuilder(mockConfigRegistry).build(mockConfig));
  }
}
