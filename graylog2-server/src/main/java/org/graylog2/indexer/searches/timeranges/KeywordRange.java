/**
 * This file is part of Graylog.
 *
 * Graylog is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.graylog2.indexer.searches.timeranges;

import com.google.common.collect.ImmutableMap;
import org.graylog2.utilities.date.NaturalDateParser;
import org.joda.time.DateTime;

import java.util.Map;

public class KeywordRange implements TimeRange {

    private final String keyword;
    private final DateTime from;
    private final DateTime to;

    @Override
    public Type getType() {
        return Type.KEYWORD;
    }

    public KeywordRange(String keyword) throws InvalidRangeParametersException {
        if (keyword == null || keyword.isEmpty()) {
            throw new InvalidRangeParametersException();
        }
        try {
            NaturalDateParser.Result result = new NaturalDateParser().parse(keyword);
            from = result.getFrom();
            to = result.getTo();
        } catch (NaturalDateParser.DateNotParsableException e) {
            throw new InvalidRangeParametersException("Could not parse from natural date: " + keyword);
        }

        this.keyword = keyword;
    }

    @Override
    public Map<String, Object> getPersistedConfig() {
        return ImmutableMap.<String, Object>builder()
                .put("type", getType().toString().toLowerCase())
                .put("keyword", getKeyword())
                .build();
    }

    public String getKeyword() {
        return keyword;
    }

    public DateTime getFrom() {
        return from;
    }

    public DateTime getTo() {
        return to;
    }
}

