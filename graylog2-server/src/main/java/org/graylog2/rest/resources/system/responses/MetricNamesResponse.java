package org.graylog2.rest.resources.system.responses;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;

public class MetricNamesResponse {
    private final MetricRegistry metricRegistry;

    public MetricNamesResponse(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    @JsonProperty
    public Collection<String> getNames() {
        return metricRegistry.getNames();
    }
}
