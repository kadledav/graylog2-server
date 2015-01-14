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
package org.graylog2.configuration;

import com.github.joschi.jadconfig.Parameter;

import java.net.URI;

public class VersionCheckConfiguration {
    @Parameter(value = "versionchecks")
    private boolean enabled = true;

    @Parameter(value = "versionchecks_uri")
    private URI uri = URI.create("http://versioncheck.torch.sh/check");

    public boolean isEnabled() {
        return enabled;
    }

    public URI getUri() {
        return uri;
    }
}
