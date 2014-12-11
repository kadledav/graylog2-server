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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.bson.types.ObjectId;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.streams.Stream;
import org.graylog2.plugin.streams.StreamRule;
import org.graylog2.plugin.streams.StreamRuleType;
import org.graylog2.streams.matchers.StreamRuleMock;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class StreamRouterEngineTest {
    @BeforeMethod
    public void setUp() throws Exception {

    }

    @Test
    public void testPresenceMatch() throws Exception {
        final StreamMock stream = getStreamMock("test");
        final StreamRuleMock rule = new StreamRuleMock(ImmutableMap.<String, Object>of(
                "_id", new ObjectId(),
                "field", "testfield",
                "type", StreamRuleType.PRESENCE.toInteger(),
                "stream_id", stream.getId()
        ));

        stream.setStreamRules(Lists.<StreamRule>newArrayList(rule));

        final StreamRouterEngine engine = new StreamRouterEngine(Lists.<Stream>newArrayList(stream));
        final Message message = getMessage();

        List<Stream> match;

        // Without testfield in the message.
        match = engine.match(message);

        assertTrue(match.isEmpty());

        // With field in the message.
        message.addField("testfield", "testvalue");

        match = engine.match(message);

        assertFalse(match.isEmpty());
    }

    @Test
    public void testExactMatch() throws Exception {
        final StreamMock stream = getStreamMock("test");
        final StreamRuleMock rule = new StreamRuleMock(ImmutableMap.<String, Object>of(
                "_id", new ObjectId(),
                "field", "testfield",
                "value", "testvalue",
                "type", StreamRuleType.EXACT.toInteger(),
                "stream_id", stream.getId()
        ));

        stream.setStreamRules(Lists.<StreamRule>newArrayList(rule));

        final StreamRouterEngine engine = new StreamRouterEngine(Lists.<Stream>newArrayList(stream));
        final Message message = getMessage();

        List<Stream> match;

        // With wrong value for field.
        message.addField("testfield", "no-testvalue");

        match = engine.match(message);

        assertTrue(match.isEmpty());

        // With matching value for field.
        message.addField("testfield", "testvalue");

        match = engine.match(message);

        assertFalse(match.isEmpty());
    }

    @Test
    public void testGreaterMatch() throws Exception {
        final StreamMock stream = getStreamMock("test");
        final StreamRuleMock rule = new StreamRuleMock(ImmutableMap.<String, Object>of(
                "_id", new ObjectId(),
                "field", "testfield",
                "value", "1",
                "type", StreamRuleType.GREATER.toInteger(),
                "stream_id", stream.getId()
        ));

        stream.setStreamRules(Lists.<StreamRule>newArrayList(rule));

        final StreamRouterEngine engine = new StreamRouterEngine(Lists.<Stream>newArrayList(stream));
        final Message message = getMessage();

        List<Stream> match;

        // With smaller value.
        message.addField("testfield", "1");

        match = engine.match(message);

        assertTrue(match.isEmpty());

        // With greater value.
        message.addField("testfield", "2");

        match = engine.match(message);

        assertFalse(match.isEmpty());
    }

    @Test
    public void testSmallerMatch() throws Exception {
        final StreamMock stream = getStreamMock("test");
        final StreamRuleMock rule = new StreamRuleMock(ImmutableMap.<String, Object>of(
                "_id", new ObjectId(),
                "field", "testfield",
                "value", "5",
                "type", StreamRuleType.SMALLER.toInteger(),
                "stream_id", stream.getId()
        ));

        stream.setStreamRules(Lists.<StreamRule>newArrayList(rule));

        final StreamRouterEngine engine = new StreamRouterEngine(Lists.<Stream>newArrayList(stream));
        final Message message = getMessage();

        List<Stream> match;

        // With bigger value.
        message.addField("testfield", "5");

        match = engine.match(message);

        assertTrue(match.isEmpty());

        // With smaller value.
        message.addField("testfield", "2");

        match = engine.match(message);

        assertFalse(match.isEmpty());
    }

    @Test
    public void testRegexMatch() throws Exception {
        final StreamMock stream = getStreamMock("test");
        final StreamRuleMock rule = new StreamRuleMock(ImmutableMap.<String, Object>of(
                "_id", new ObjectId(),
                "field", "testfield",
                "value", "^test",
                "type", StreamRuleType.REGEX.toInteger(),
                "stream_id", stream.getId()
        ));

        stream.setStreamRules(Lists.<StreamRule>newArrayList(rule));

        final StreamRouterEngine engine = new StreamRouterEngine(Lists.<Stream>newArrayList(stream));
        final Message message = getMessage();

        List<Stream> match;

        // With non-matching value.
        message.addField("testfield", "notestvalue");

        match = engine.match(message);

        assertTrue(match.isEmpty());

        // With matching value.
        message.addField("testfield", "testvalue");

        match = engine.match(message);

        assertFalse(match.isEmpty());
    }

    private StreamMock getStreamMock(String title) {
        return new StreamMock(ImmutableMap.<String, Object>of("_id", new ObjectId(), "title", title));
    }

    private Message getMessage() {
        return new Message("test message", "localhost", new DateTime());
    }
}