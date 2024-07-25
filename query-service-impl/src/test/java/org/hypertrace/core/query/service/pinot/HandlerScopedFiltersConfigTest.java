package org.hypertrace.core.query.service.pinot;

import com.typesafe.config.ConfigFactory;
import java.util.Optional;
import org.hypertrace.core.query.service.HandlerScopedFiltersConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HandlerScopedFiltersConfigTest {

  @Test
  void testTimeBoundaryAndFilter() {
    String filterConfig =
        "tenantScopedFilters = [\n"
            + "          {\n"
            + "            \"tenantId\" = \"__default\"\n"
            + "            \"timeRangeAndFilters\" = [\n"
            + "              {\n"
            + "                \"startTimeMillis\" = 1\n"
            + "                \"endTimeMillis\" = 2\n"
            + "                \"filter\" : \"{\\\"lhs\\\": {\\\"attributeExpression\\\": {\\\"attributeId\\\": \\\"SERVICE.name\\\"}}, \\\"operator\\\": \\\"EQ\\\", \\\"rhs\\\": {\\\"literal\\\": {\\\"value\\\": {\\\"string\\\": \\\"dummyService\\\"}}}}\"\n"
            + "              }\n"
            + "            ]\n"
            + "           }\n"
            + "        ]";

    HandlerScopedFiltersConfig handlerScopedFiltersConfig =
        new HandlerScopedFiltersConfig(
            ConfigFactory.parseString(filterConfig), Optional.of("Event.startTime"));
    Assertions.assertEquals(
        1, handlerScopedFiltersConfig.getAdditionalFiltersForTenant("__default").size());
    Assertions.assertEquals(
        0, handlerScopedFiltersConfig.getAdditionalFiltersForTenant("otherTenant").size());
  }

  @Test
  void testMultipleTimeBoundaryAndFilter() {
    String filterConfig =
        "tenantScopedFilters = [\n"
            + "          {\n"
            + "            \"tenantId\" = \"__default\"\n"
            + "            \"timeRangeAndFilters\" = [\n"
            + "              {\n"
            + "                \"startTimeMillis\" = 1\n"
            + "                \"endTimeMillis\" = 2\n"
            + "                \"filter\" : \"{\\\"lhs\\\": {\\\"attributeExpression\\\": {\\\"attributeId\\\": \\\"SERVICE.name\\\"}}, \\\"operator\\\": \\\"EQ\\\", \\\"rhs\\\": {\\\"literal\\\": {\\\"value\\\": {\\\"string\\\": \\\"dummyService\\\"}}}}\"\n"
            + "              }\n"
            + "              {\n"
            + "                              \"startTimeMillis\" = 1\n"
            + "                              \"endTimeMillis\" = 2\n"
            + "                              \"filter\" : \"{\\\"lhs\\\": {\\\"attributeExpression\\\": {\\\"attributeId\\\": \\\"SERVICE.name\\\"}}, \\\"operator\\\": \\\"EQ\\\", \\\"rhs\\\": {\\\"literal\\\": {\\\"value\\\": {\\\"string\\\": \\\"dummyService\\\"}}}}\"\n"
            + "              }\n"
            + "            ]\n"
            + "           }\n"
            + "        ]";

    HandlerScopedFiltersConfig handlerScopedFiltersConfig =
        new HandlerScopedFiltersConfig(
            ConfigFactory.parseString(filterConfig), Optional.of("Event.startTime"));
    Assertions.assertEquals(
        2, handlerScopedFiltersConfig.getAdditionalFiltersForTenant("__default").size());
  }

  @Test
  void testMultipleTenantsFilters() {
    String filterConfig =
        "tenantScopedFilters = [\n"
            + "                  {\n"
            + "                    \"tenantId\" = \"__default\"\n"
            + "                    \"timeRangeAndFilters\" = [\n"
            + "                      {\n"
            + "                        \"startTimeMillis\" = 1\n"
            + "                        \"endTimeMillis\" = 2\n"
            + "                      }\n"
            + "                    ]\n"
            + "                  },\n"
            + "                  {\n"
            + "                                      \"tenantId\" = \"tenant2\"\n"
            + "                                      \"timeRangeAndFilters\" = [\n"
            + "                                        {\n"
            + "                                          \"startTimeMillis\" = 1\n"
            + "                                          \"endTimeMillis\" = 2\n"
            + "                                        }\n"
            + "                                      ]\n"
            + "                                    }\n"
            + "                ]";

    HandlerScopedFiltersConfig handlerScopedFiltersConfig =
        new HandlerScopedFiltersConfig(
            ConfigFactory.parseString(filterConfig), Optional.of("Event.startTime"));
    Assertions.assertEquals(
        1, handlerScopedFiltersConfig.getAdditionalFiltersForTenant("__default").size());
    Assertions.assertEquals(
        1, handlerScopedFiltersConfig.getAdditionalFiltersForTenant("tenant2").size());
  }
}
