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

import java.text.DecimalFormatSymbols;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.types.ObjectId;
import org.envirocar.server.core.dao.SensorDao;
import org.envirocar.server.core.entities.Sensor;
import org.envirocar.server.core.entities.Sensors;
import org.envirocar.server.core.filter.PropertyFilter;
import org.envirocar.server.core.filter.SensorFilter;
import org.envirocar.server.core.util.Pagination;
import org.envirocar.server.mongo.MongoDB;
import org.envirocar.server.mongo.entity.MongoMeasurement;
import org.envirocar.server.mongo.entity.MongoSensor;
import org.envirocar.server.mongo.util.MongoUtils;

import com.github.jmkgreen.morphia.Key;
import com.github.jmkgreen.morphia.mapping.Mapper;
import com.github.jmkgreen.morphia.query.Query;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

/**
 * TODO JavaDoc
 *
 * @author Christian Autermann <autermann@uni-muenster.de>
 */
public class MongoSensorDao extends AbstractMongoDao<ObjectId, MongoSensor, Sensors>
        implements SensorDao {
    
    private MongoMeasurementDao measurementDao;
    
    @Inject
    public MongoSensorDao(MongoDB mongoDB) {
        super(MongoSensor.class, mongoDB);
    }
    
    public MongoMeasurementDao getMeasurementDao() {
        return measurementDao;
    }

    @Inject
    public void setMeasurementDao(MongoMeasurementDao measurementDao) {
        this.measurementDao = measurementDao;
    }

    @Override
    public Sensor getByIdentifier(String id) {
        ObjectId oid;
        try {
            oid = new ObjectId(id);
        } catch (IllegalArgumentException e) {
            return null;
        }
        return get(oid);
    }
    
    @Override
    public Collection<String> getTypes() {
        return getDistinctStringField(MongoSensor.TYPE);
    }

    @Override
    public Sensors get(SensorFilter request) {
        Query<MongoSensor> q = q();
        if (request.hasType()) {
            q.field(MongoSensor.TYPE).equal(request.getType());
        }
        if (request.hasFilters()) {
            applyFilters(q, request.getFilters());
        }
        return fetch(q, request.getPagination());
    }

    @Override
    public Sensors get(Pagination p) {
        return fetch(q(), p);
    }

    @Override
    public Sensor create(Sensor sensor) {
        MongoSensor ms = (MongoSensor) sensor;
        save(ms);
        return ms;
    }

    @Override
    protected Sensors createPaginatedIterable(Iterable<MongoSensor> i,
                                              Pagination p, long count) {
        return Sensors.from(i).withElements(count).withPagination(p).build();
    }

    private Query<MongoSensor> applyFilters(Query<MongoSensor> q,
                                            Set<PropertyFilter> filters) {
        if (filters == null || filters.isEmpty()) {
            return q;
        }
        Multimap<String, Object> map = LinkedListMultimap.create();
        for (PropertyFilter f : filters) {
            String field = f.getField();
            String value = f.getValue();
            // "123" != 123 && "true" != true in MongoDB...
            if (field != null && value != null) {
                field = MongoUtils.path(MongoSensor.PROPERTIES, field);
                if (isTrue(value)) {
                    map.put(field, true);
                } else if (isFalse(value)) {
                    map.put(field, false);
                } else if (isNumeric(value)) {
                    map.put(field, Double.valueOf(value));
                } else {
                    map.put(field, value);
                }
            }
        }
        q.disableValidation();
        for (String field : map.keySet()) {
            q.field(field).in(map.get(field));
        }
        return q.enableValidation();
    }

    public boolean isTrue(String str) {
        return str.equalsIgnoreCase("true");
    }

    public boolean isFalse(String str) {
        return str.equalsIgnoreCase("false");
    }

    public boolean isNumeric(String str) {
        DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
        char localeMinusSign = symbols.getMinusSign();

        if (!Character.isDigit(str.charAt(0)) &&
            str.charAt(0) != localeMinusSign) {
            return false;
        }

        boolean isDecimalSeparatorFound = false;
        char localeDecimalSeparator = symbols.getDecimalSeparator();

        for (char c : str.substring(1).toCharArray()) {
            if (!Character.isDigit(c)) {
                if (c == localeDecimalSeparator && !isDecimalSeparatorFound) {
                    isDecimalSeparatorFound = true;
                    continue;
                }
                return false;
            }
        }
        return true;
    }

    public List<Key<MongoSensor>> getKeys(SensorFilter request) {
        Query<MongoSensor> q = q();
        if (request.hasType()) {
            q.field(MongoSensor.TYPE).equal(request.getType());
        }
        return q.asKeyList();
    }

    @Override
    public Map<String, Collection<String>> getSensorPhenomenonsMap() {
        Map<String, Collection<String>> phenMap = Maps.newHashMap();
        for (DBObject dbo : measurementDao.getDistinctPhenomenons()) {
            phenMap.put(dbo.get(Mapper.ID_KEY).toString(), toList((BasicDBList)dbo.get(MongoMeasurement.PHENOMENONS)));
        }
        return phenMap;
    }

    @Override
    protected AggregationOutput aggregate(DBObject firstOp, DBObject... additionalOps) {
        return aggregate(MongoSensor.class, firstOp, additionalOps);
    }


}
