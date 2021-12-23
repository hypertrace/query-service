package org.hypertrace.core.query.service.attribubteexpression;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import org.hypertrace.core.query.service.QueryTransformation;

public class ExpressionModule extends AbstractModule {

  @Override
  protected void configure() {
    Multibinder<QueryTransformation> transformationMultibinder =
        Multibinder.newSetBinder(binder(), QueryTransformation.class);
    transformationMultibinder.addBinding().to(AttributeSubpathExistsFilteringTransformation.class);
    transformationMultibinder.addBinding().to(AttributeNormalizationTransformation.class);
  }
}
