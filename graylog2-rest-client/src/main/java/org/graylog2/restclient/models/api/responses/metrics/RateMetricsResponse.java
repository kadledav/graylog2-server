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
package org.graylog2.restclient.models.api.responses.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Lennart Koopmann <lennart@torch.sh>
 */
public class RateMetricsResponse {

    public double total;
    public double mean;

    @JsonProperty("one_minute")
    public double oneMinute;

    @JsonProperty("five_minute")
    public double fiveMinute;

    @JsonProperty("fifteen_minute")
    public double fifteenMinute;

}
