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
package org.envirocar.server.mongo.dao;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.envirocar.server.core.dao.PhenomenonDao;
import org.envirocar.server.core.entities.Phenomenon;
import org.envirocar.server.core.entities.Phenomenons;
import org.envirocar.server.core.util.Pagination;
import org.envirocar.server.mongo.MongoDB;
import org.envirocar.server.mongo.entity.MongoMeasurement;
import org.envirocar.server.mongo.entity.MongoPhenomenon;

import com.github.jmkgreen.morphia.mapping.Mapper;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

/**
 * TODO JavaDoc
 *
 * @author Christian Autermann <autermann@uni-muenster.de>
 * @author Jan Wirwahn <jan.wirwahn@wwu.de>
 */
public class MongoPhenomenonDao extends AbstractMongoDao<String, MongoPhenomenon, Phenomenons>
        implements PhenomenonDao {
    
    private MongoMeasurementDao measurementDao;
    
    @Inject
    public MongoPhenomenonDao(MongoDB mongoDB) {
        super(MongoPhenomenon.class, mongoDB);
    }
    
    public MongoMeasurementDao getMeasurementDao() {
        return measurementDao;
    }

    @Inject
    public void setMeasurementDao(MongoMeasurementDao measurementDao) {
        this.measurementDao = measurementDao;
    }

    @Override
    public MongoPhenomenon getByName(final String name) {
        return q().field(MongoPhenomenon.NAME).equal(name).get();
    }

    @Override
    public Phenomenons get(Pagination p) {
        return fetch(q(), p);
    }
    
    @Override
    public Map<String, Collection<String>> getPhenomenonSensorsMap() {
        Map<String, Collection<String>> sensorMap = Maps.newHashMap();
        for (DBObject dbo : measurementDao.getDistinctSensors()) {
            sensorMap.put(dbo.get(Mapper.ID_KEY).toString(), toList((BasicDBList)dbo.get(MongoMeasurement.SENSORS)));
        }
        return sensorMap;
    }

    @Override
    public Collection<String> getPhenomenonsForSensorId(String sensorId) {
        Set<String> phenomenons = Sets.newHashSet();
        for (DBObject dbo : measurementDao.getDistinctPhenomenons(sensorId)) {
            phenomenons.add(dbo.get(Mapper.ID_KEY).toString());
        }
        return phenomenons;
    }
    
    @Override
    public Collection<String> getIdentifier() {
        return getDistinctStringField(MongoPhenomenon.NAME);
    }
    
    @Override
    public MongoPhenomenon create(Phenomenon phen) {
        MongoPhenomenon ph = (MongoPhenomenon) phen;
        save(ph);
        return ph;
    }

    @Override
    protected Phenomenons createPaginatedIterable(Iterable<MongoPhenomenon> i,
                                                  Pagination p, long count) {
        return Phenomenons.from(i).withPagination(p).withElements(count).build();
    }

    @Override
    protected AggregationOutput aggregate(DBObject firstOp, DBObject... additionalOps) {
        return aggregate(MongoPhenomenon.class, firstOp, additionalOps);
    }
}
