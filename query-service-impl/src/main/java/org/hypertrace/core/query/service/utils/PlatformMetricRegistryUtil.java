package org.hypertrace.core.query.service.utils;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.Tag;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

//this should be added in utils repo or change should be made in platform metrics file of service framework repo
public class PlatformMetricRegistryUtil extends PlatformMetricsRegistry {

    public static Iterable<Tag> getTags(Map<String, String> tags) {
        if (tags != null && !tags.isEmpty()) {
            Set<Tag> newTags = new HashSet();
            tags.forEach((k, v) -> {
                newTags.add(new ImmutableTag(k, v));
            });
            return newTags;
        } else {
            return new HashSet();
        }
    }

    public static DistributionSummary registerDistributionSummary(String name, Map<String, String> tags, boolean histogram) {
        io.micrometer.core.instrument.DistributionSummary.Builder builder = DistributionSummary.builder(name).tags(getTags(tags)).maximumExpectedValue(3000.0);
        if(histogram) {
            builder.publishPercentileHistogram();
        }
        return builder.register(getMeterRegistry());
    }

}
