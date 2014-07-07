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

import org.envirocar.server.core.TemporalFilter;
import org.envirocar.server.core.entities.Phenomenon;
import org.envirocar.server.core.entities.Sensor;
import org.envirocar.server.core.entities.Track;
import org.envirocar.server.core.entities.User;
import org.envirocar.server.core.util.Pagination;

import com.vividsolutions.jts.geom.Geometry;

/**
 * TODO JavaDoc
 *
 * @author Christian Autermann <autermann@uni-muenster.de>
 */
public class MeasurementFilter {
    private final Track track;
    private final User user;
    private Geometry geometry;
    private final Pagination pagination;
    private TemporalFilter temporalFilter;
    private Iterable<Sensor> sensors;
    private Iterable<Phenomenon> phenomenon;
    private Iterable<Track> tracks;
    private Collection<String> trackIds;
    private Collection<String> phenomenonIds;
    private Collection<String> sensorIds;
    
    public MeasurementFilter() {
        this(null);
    }

    public MeasurementFilter(Track t, User u, Geometry g,
                             TemporalFilter tf, Pagination p) {
        this.track = t;
        this.user = u;
        this.geometry = g;
        this.pagination = p;
        this.temporalFilter = tf;
    }

    public MeasurementFilter(Track t, User u, Geometry g, Pagination p) {
        this(t, u, g, null, p);
    }

    public MeasurementFilter(Geometry g, Pagination p) {
        this(null, null, g, null, p);
    }

    public MeasurementFilter(Track t, Geometry g, Pagination p) {
        this(t, null, g, null, p);
    }

    public MeasurementFilter(User u, Geometry g, Pagination p) {
        this(null, u, g, null, p);
    }

    public MeasurementFilter(Track t, Pagination p) {
        this(t, null, null, null, p);
    }

    public MeasurementFilter(User u, Pagination p) {
        this(null, u, null, null, p);
    }

    public MeasurementFilter(Track t) {
        this(t, null, null, null, null);
    }

    public Track getTrack() {
        return track;
    }

    public boolean hasTrack() {
        return track != null;
    }

    public User getUser() {
        return user;
    }

    public boolean hasUser() {
        return user != null;
    }

    public Geometry getGeometry() {
        return geometry;
    }

    public boolean hasGeometry() {
        return geometry != null;
    }

    public Pagination getPagination() {
        return pagination;
    }

    public boolean hasPagination() {
        return pagination != null;
    }
    
    public MeasurementFilter setTemporalFilter(TemporalFilter tf) {
        this.temporalFilter = tf;
        return this;
    }

    public TemporalFilter getTemporalFilter() {
        return temporalFilter;
    }

    public boolean hasTemporalFilter() {
        return temporalFilter != null;
    }

    public MeasurementFilter setSensors(Iterable<Sensor> sensors) {
        this.sensors = sensors;
        return this;
    }
    public Iterable<Sensor> getSensors() {
        return sensors;
    }
    
    public boolean hasSensors() {
        return getSensors() != null;
    }
    
    public MeasurementFilter setPhenomeon(Iterable<Phenomenon> phenomenon) {
        this.phenomenon = phenomenon;
        return this;
    }

    public Iterable<Phenomenon> getPhenomenon() {
        return phenomenon;
    }
    
    public boolean hasPhenomeon() {
        return getPhenomenon() != null;
    }
    
    public MeasurementFilter setTracks(Collection<Track> tracks) {
        this.tracks = tracks;
        return this;
    }

    public Iterable<Track> getTracks() {
        return tracks;
    }
    
    public boolean hasTracks() {
        return getTracks() != null;
    }

    public void setGeometry(Geometry geometry) {
        this.geometry = geometry;
    }
    
    public void setTrackIds(Collection<String> trackIds) {
       this.trackIds = trackIds;
    }

    public void setPhenomenonIds(Collection<String> phenomenonIds) {
        this.phenomenonIds = phenomenonIds;
    }

    public void setSensorIds(Collection<String> sensorIds) {
        this.sensorIds = sensorIds;
    }

    public boolean hasSensorIds() {
        return getSensorIds() != null && !getSensorIds().isEmpty();
    }

    public boolean hasPhenomenonIds() {
        return getPhenomenonIds() != null && !getPhenomenonIds().isEmpty();
    }

    public boolean hasTrackIds() {
        return getTrackIds() != null && !getTrackIds().isEmpty();
    }

    public Collection<String> getSensorIds() {
        return sensorIds;
    }

    public Collection<String> getPhenomenonIds() {
        return phenomenonIds;
    }

    public Collection<String> getTrackIds() {
        return trackIds;
    }
}
