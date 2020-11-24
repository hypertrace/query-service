package org.hypertrace.core.query.service.projection;

import javax.inject.Provider;
import org.hypertrace.core.attribute.service.projection.AttributeProjectionRegistry;

class ProjectionRegistryProvider implements Provider<AttributeProjectionRegistry> {
  @Override
  public AttributeProjectionRegistry get() {
    return new AttributeProjectionRegistry();
  }
}
