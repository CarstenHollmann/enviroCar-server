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

import com.google.common.collect.Sets;
import com.vividsolutions.jts.geom.Geometry;

public class TrackMeasurementFilter {
    
    private Collection<String> sensorIds;
    
    private Collection<String> phenomenonIds;
    
    private Collection<Geometry> geometries;

    public TrackMeasurementFilter setSensorIds(Collection<String> sensorIds) {
        this.sensorIds = sensorIds;
        return this;
    }
    
    public Collection<String> getSensorIds() {
        return sensorIds;
    }
    
    public boolean hasSensorIds() {
        return getSensorIds() != null && !getSensorIds().isEmpty();
    }
 
    public TrackMeasurementFilter setPhenomenonIds(Collection<String> phenomenonIds) {
        this.phenomenonIds = phenomenonIds;
        return this;
    }
    
    public Collection<String> getPhenomenonIds() {
        return phenomenonIds;
    }
    
    public boolean hasPhenomenonIds() {
        return getPhenomenonIds() != null && !getPhenomenonIds().isEmpty();
    }

    public TrackMeasurementFilter addGeometry(Geometry geometry) {
        if (getGeometries() == null) {
            this.geometries = Sets.newHashSet();
        }
        getGeometries().add(geometry);
        return this;
    }
    
    public Collection<Geometry> getGeometries() {
        return geometries;
    }
    
    public boolean hasGeometries() {
        return getGeometries() != null && !getGeometries().isEmpty();
    }

}
