package org.graylog2.rest.resources.system.responses;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Maps;

import java.util.Map;

@JsonAutoDetect
public class MetricsResponse {
    private final MetricRegistry metricRegistry;
    private Predicate<String> filter = Predicates.alwaysTrue();

    public MetricsResponse(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    public void setMetricFilter(Predicate<String> filter) {
        this.filter = filter;
    }

    @JsonProperty
    public Map<String, Metric> getMetrics() {
        final Map<String, Metric> metrics = metricRegistry.getMetrics();
        return Maps.filterKeys(metrics, filter);
    }

    @JsonProperty
    public int getTotal() {
        return getMetrics().size();
    }
}
