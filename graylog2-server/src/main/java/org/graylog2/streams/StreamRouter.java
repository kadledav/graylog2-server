/**
 * This file is part of Graylog2.
 *
 * Graylog2 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog2 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog2.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.graylog2.streams;

import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.InstrumentedThreadFactory;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.inject.name.Named;
import org.graylog2.Configuration;
import org.graylog2.database.NotFoundException;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.streams.Stream;
import org.graylog2.plugin.streams.StreamRule;
import org.graylog2.streams.matchers.StreamRuleMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Routes a {@link org.graylog2.plugin.Message} to its streams.
 */
public class StreamRouter {
    private static final Logger LOG = LoggerFactory.getLogger(StreamRouter.class);

    protected final StreamService streamService;
    protected final StreamRuleService streamRuleService;
    private final MetricRegistry metricRegistry;
    private final StreamMetrics streamMetrics;
    private final StreamFaultManager streamFaultManager;
    private final Configuration configuration;
    private final ServerStatus serverStatus;

    private final ExecutorService executor;
    private final TimeLimiter timeLimiter;
    private final AtomicReference<StreamRouterEngine> routerEngine = new AtomicReference<>(null);

    final private ConcurrentMap<String, AtomicInteger> faultCounter;

    @Inject
    public StreamRouter(StreamService streamService,
                        StreamRuleService streamRuleService,
                        MetricRegistry metricRegistry,
                        StreamMetrics streamMetrics,
                        StreamFaultManager streamFaultManager,
                        Configuration configuration,
                        ServerStatus serverStatus,
                        StreamRouterEngine.Factory routerEngineFactory,
                        @Named("daemonScheduler") ScheduledExecutorService scheduler) {
        this.streamService = streamService;
        this.streamRuleService = streamRuleService;
        this.metricRegistry = metricRegistry;
        this.streamMetrics = streamMetrics;
        this.streamFaultManager = streamFaultManager;
        this.configuration = configuration;
        this.serverStatus = serverStatus;
        this.faultCounter = Maps.newConcurrentMap();
        this.executor = executorService();
        this.timeLimiter = new SimpleTimeLimiter(executor);

        final StreamRouterEngineUpdater streamRouterEngineUpdater = new StreamRouterEngineUpdater(routerEngine, routerEngineFactory, streamService, executor);
        this.routerEngine.set(streamRouterEngineUpdater.getNewEngine());
        scheduler.scheduleAtFixedRate(streamRouterEngineUpdater, 0, 1, TimeUnit.SECONDS);
    }

    private ExecutorService executorService() {
        return new InstrumentedExecutorService(Executors.newCachedThreadPool(threadFactory()), metricRegistry,
                name(this.getClass(), "executor-service"));
    }

    private ThreadFactory threadFactory() {
        return new InstrumentedThreadFactory(new ThreadFactoryBuilder()
                .setNameFormat("stream-router-%d")
                .setDaemon(true)
                .build(), metricRegistry);
    }

    private AtomicInteger getFaultCount(String streamId) {
        faultCounter.putIfAbsent(streamId, new AtomicInteger());
        return faultCounter.get(streamId);
    }

    public List<Stream> route(final Message msg) {
        return routerEngine.get().match(msg);
    }

    public List<Stream> routeOld(final Message msg) {
        final List<Stream> matches = Lists.newArrayList();
        final List<Stream> streams = getStreams();
        msg.recordCounter(serverStatus, "streams-evaluated", streams.size());

        final long timeout = configuration.getStreamProcessingTimeout();
        final int maxFaultCount = configuration.getStreamProcessingMaxFaults();

        for (final Stream stream : streams) {
            final Timer timer = streamMetrics.getExecutionTimer(stream.getId());

            final Callable<Boolean> task = new Callable<Boolean>() {
                public Boolean call() {
                    final Map<StreamRule, Boolean> result = getRuleMatches(stream, msg);
                    return doesStreamMatch(result);
                }
            };

            try (final Timer.Context ignored = timer.time()) {
                final boolean matched = timeLimiter.callWithTimeout(task, timeout, TimeUnit.MILLISECONDS, true);
                if (matched) {
                    streamMetrics.markIncomingMeter(stream.getId());
                    matches.add(stream);
                }
            } catch (Exception e) {
                streamFaultManager.registerFailure(stream);
            }
        }

        return matches;
    }

    List<Stream> getStreams() {
        return streamService.loadAllEnabled();
    }

    List<StreamRule> getStreamRules(Stream stream) {
        try {
            return streamRuleService.loadForStream(stream);
        } catch (NotFoundException e) {
            LOG.error("Caught exception while fetching stream rules", e);
            return Collections.emptyList();
        }
    }

    public Map<StreamRule, Boolean> getRuleMatches(Stream stream, Message msg) {
        final Map<StreamRule, Boolean> result = Maps.newHashMap();

        final List<StreamRule> streamRules = getStreamRules(stream);

        int evaluatedRulesCount = 0;
        for (final StreamRule rule : streamRules) {
            evaluatedRulesCount++;
            try {
                final StreamRuleMatcher matcher = StreamRuleMatcherFactory.build(rule.getType());
                result.put(rule, matchStreamRule(msg, matcher, rule));
            } catch (InvalidStreamRuleTypeException e) {
                LOG.warn("Invalid stream rule type. Skipping matching for this rule. " + e.getMessage(), e);
            }
        }
        msg.recordCounter(serverStatus, "streamrules-evaluated-" + stream.getId(), evaluatedRulesCount);

        return result;
    }

    public boolean doesStreamMatch(Map<StreamRule, Boolean> ruleMatches) {
        return !ruleMatches.isEmpty() && !ruleMatches.values().contains(false);
    }

    public boolean matchStreamRule(Message msg, StreamRuleMatcher matcher, StreamRule rule) {
        try {
            return matcher.match(msg, rule);
        } catch (Exception e) {
            LOG.debug("Could not match stream rule <" + rule.getType() + "/" + rule.getValue() + ">: " + e.getMessage(), e);
            streamMetrics.markExceptionMeter(rule.getStreamId());
            return false;
        }
    }

    private class StreamRouterEngineUpdater implements Runnable {
        private final AtomicReference<StreamRouterEngine> routerEngine;
        private final StreamRouterEngine.Factory engineFactory;
        private final StreamService streamService;
        private final ExecutorService executorService;

        public StreamRouterEngineUpdater(AtomicReference<StreamRouterEngine> routerEngine,
                                         StreamRouterEngine.Factory engineFactory,
                                         StreamService streamService,
                                         ExecutorService executorService) {
            this.routerEngine = routerEngine;
            this.engineFactory = engineFactory;
            this.streamService = streamService;
            this.executorService = executorService;
        }

        @Override
        public void run() {
            final StreamRouterEngine engine = getNewEngine();

            LOG.debug("Updating to new router engine");
            // TODO Add fingerprint to engine to avoid setting this on every invocation. Compare fingerprint, if not changed, do not update.
            routerEngine.set(engine);
        }

        private StreamRouterEngine getNewEngine() {
            return engineFactory.create(streamService.loadAllEnabled(), executorService);
        }
    }
}
