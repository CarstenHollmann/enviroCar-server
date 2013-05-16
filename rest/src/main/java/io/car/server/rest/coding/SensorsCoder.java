/**
 * Copyright (C) 2013  Christian Autermann, Jan Alexander Wirwahn,
 *                     Arne De Wall, Dustin Demuth, Saqib Rasheed
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
package io.car.server.rest.coding;


import javax.ws.rs.core.MediaType;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.inject.Inject;

import io.car.server.core.entities.Sensor;
import io.car.server.core.entities.Sensors;

/**
 * @author Christian Autermann <c.autermann@52north.org>
 */
public class SensorsCoder implements EntityEncoder<Sensors> {
    private final EntityEncoder<Sensor> sensorEncoder;

    @Inject
    public SensorsCoder(EntityEncoder<Sensor> sensorEncoder) {
        this.sensorEncoder = sensorEncoder;
    }

    @Override
    public JSONObject encode(Sensors t, MediaType mediaType) throws JSONException {
        JSONArray a = new JSONArray();
        for (Sensor u : t) {
            a.put(sensorEncoder.encode(u, mediaType));
        }
        return new JSONObject().put(JSONConstants.SENSORS_KEY, a);
    }
}
