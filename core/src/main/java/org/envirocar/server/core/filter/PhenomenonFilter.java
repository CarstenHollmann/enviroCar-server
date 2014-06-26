/*
 * Copyright (C) 2013 The enviroCar project
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.envirocar.server.core.filter;

import java.util.Collection;

import org.envirocar.server.core.entities.Phenomenon;
import org.envirocar.server.core.entities.Sensor;
import org.envirocar.server.core.entities.Sensors;
import org.envirocar.server.core.entities.Track;

public class PhenomenonFilter {
    
    private Iterable<Sensor> sensors;
    
    private Iterable<Phenomenon> phenomenon;
    
    private Iterable<Track> tracks;

    
    public PhenomenonFilter setSensors(Sensors sensors) {
        this.sensors = sensors;
        return this;
    }
    
    public Iterable<Sensor> getSensors() {
        return sensors;
    }
    
    public boolean hasSensors() {
        return getSensors() != null;
    }
    
    public PhenomenonFilter setPhenomeon(Collection<Phenomenon> phenomenon) {
        this.phenomenon = phenomenon;
        return this;
    }

    public Iterable<Phenomenon> getPhenomenon() {
        return phenomenon;
    }
    
    public boolean hasPhenomeon() {
        return getPhenomenon() != null;
    }
    
    public PhenomenonFilter setTracks(Collection<Track> tracks) {
        this.tracks = tracks;
        return this;
    }

    public Iterable<Track> getTracks() {
        return tracks;
    }
    
    public boolean hasTracks() {
        return getTracks() != null;
    }
}
