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

import static org.envirocar.server.mongo.dao.MongoMeasurementDao.ID;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.types.ObjectId;
import org.envirocar.server.core.dao.TrackDao;
import org.envirocar.server.core.entities.Sensor;
import org.envirocar.server.core.entities.Track;
import org.envirocar.server.core.entities.Tracks;
import org.envirocar.server.core.filter.MeasurementFilter;
import org.envirocar.server.core.filter.TrackFilter;
import org.envirocar.server.core.filter.TrackMeasurementFilter;
import org.envirocar.server.core.util.GeoJSONConstants;
import org.envirocar.server.core.util.Pagination;
import org.envirocar.server.mongo.MongoDB;
import org.envirocar.server.mongo.entity.MongoMeasurement;
import org.envirocar.server.mongo.entity.MongoSensor;
import org.envirocar.server.mongo.entity.MongoTrack;
import org.envirocar.server.mongo.entity.MongoUser;
import org.envirocar.server.mongo.util.MongoUtils;
import org.envirocar.server.mongo.util.MorphiaUtils;
import org.envirocar.server.mongo.util.Ops;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jmkgreen.morphia.Key;
import com.github.jmkgreen.morphia.mapping.Mapper;
import com.github.jmkgreen.morphia.query.Query;
import com.github.jmkgreen.morphia.query.UpdateResults;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

/**
 * TODO JavaDoc
 *
 * @author Arne de Wall <a.dewall@52north.org>
 */
public class MongoTrackDao extends AbstractMongoDao<ObjectId, MongoTrack, Tracks> implements TrackDao {
    
    private static final Logger log = LoggerFactory.getLogger(MongoTrackDao.class);
    
    private static final String PHEN_START_TIME = "phenStartTime";
    
    private static final String PHEN_END_TIME = "phenEndTime";
    
    private static final String RESULT_START_TIME = "resultStartTime";
    
    private static final String RESULT_END_TIME = "resultEndTime";

    private MongoMeasurementDao measurementDao;

    private MongoSensorDao sensorDAO;

    @Inject
    public MongoTrackDao(MongoDB mongoDB) {
        super(MongoTrack.class, mongoDB);
    }

    public MongoMeasurementDao getMeasurementDao() {
        return measurementDao;
    }

    @Inject
    public void setMeasurementDao(MongoMeasurementDao measurementDao) {
        this.measurementDao = measurementDao;
    }

    @Inject
    public void setSensorDAO(MongoSensorDao sensorDAO) {
        this.sensorDAO = sensorDAO;
    }

    public MongoSensorDao getSensorDAO() {
        return sensorDAO;
    }

    @Override
    public Track getById(String id) {
        ObjectId oid;
        try {
            oid = new ObjectId(id);
        } catch (IllegalArgumentException e) {
            return null;
        }
        return get(oid);
    }

    @Override
    public Collection<String> getIdentifier() {
        Set<String> identifier = Sets.newHashSet();
        for (Track track : get()) {
            identifier.add(track.getIdentifier());
        }
        return identifier;
    }

    @Override
    public Track create(Track track) {
        return save(track);
    }

    @Override
    public MongoTrack save(Track track) {
        MongoTrack mongoTrack = (MongoTrack) track;
        save(mongoTrack);
        return mongoTrack;
    }

    @Override
    public void delete(Track track) {
        MongoTrack t = (MongoTrack) track;
        measurementDao.removeTrack(t);
        delete(t.getId());
    }

    @Override
    public Tracks get(TrackFilter request) {
        Query<MongoTrack> q = q();
        if (request.hasGeometry()) {
            List<Key<MongoTrack>> keys =
                    measurementDao.getTrackKeysByBbox(new MeasurementFilter(null, request.getUser(), request
                            .getGeometry(), null, null));
            if (keys.isEmpty()) {
                return Tracks.none();
            }
            q.field(MongoTrack.ID).in(toIdList(keys));
        } else if (request.hasUser()) {
            q.field(MongoTrack.USER).equal(key(request.getUser()));
        }
        if (request.hasTemporalFilter()) {
            MorphiaUtils.temporalFilter(q.field(MongoTrack.BEGIN), q.field(MongoTrack.END),
                    request.getTemporalFilter());
        }
        if (request.hasIdentifier()) {
            q.field(MongoTrack.ID).in(toIdList(request.getIdentifier()));
        }
        if (request.hasSensorType()) {
            q.field(MongoUtils.path(MongoTrack.SENSOR, MongoSensor.TYPE)).equal(request.getSensorType());
        }
        if (request.hasSensors()) {
            // TODO check if it works
            q.field(MongoUtils.path(MongoTrack.SENSOR, MongoSensor.NAME)).in(toObjectIdList(request.getSensors()));
        }
        return fetch(q, request.getPagination());
    }

    private Iterable<?> toObjectIdList(Collection<Sensor> sensors) {
        List<ObjectId> ids = Lists.newLinkedList();
        for (Sensor sensor : sensors) {
            ids.add(new ObjectId(sensor.getIdentifier()));
        }
        return ids;
    }

    @Override
    public Tracks get() {
        return fetch(q(), null);
    }

    @Override
    public Envelope getBBox(TrackFilter request) {
        Envelope bbox = new Envelope();
        for (Track track : get(request)) {
            if (track.hasBoundingBox()) {
                bbox.expandToInclude(track.getBoundingBox());
            }
        }
        return bbox;
    }

    @Override
    public void update(Track track) {
        updateTimestamp((MongoTrack) track);
    }

    @Override
    public void calculateBoundingBox(Track track) {

        String coordPath = MongoUtils.valueOf(MongoMeasurement.GEOMETRY, GeoJSONConstants.COORDINATES_KEY);

        AggregationOutput result =
                getMongoDB()
                        .getDatastore()
                        .getCollection(MongoMeasurement.class)
                        .aggregate(
                                bson().push(Ops.MATCH).add(MongoMeasurement.TRACK, ref(track)).get(),
                                bson().push(Ops.GROUP).add(ID, 0).push("min").add(Ops.MIN, coordPath).pop()
                                        .push("max").add(Ops.MAX, coordPath).pop().get());
        result.getCommandResult().throwOnError();

        Iterator<DBObject> it = result.results().iterator();
        if (!it.hasNext()) {
            track.setBoundingBox(null);
        } else {
            DBObject r = it.next();
            BasicDBList min = (BasicDBList) r.get("min");
            BasicDBList max = (BasicDBList) r.get("max");
            double minX = ((Number) min.get(0)).doubleValue();
            double minY = ((Number) min.get(1)).doubleValue();
            double maxX = ((Number) max.get(0)).doubleValue();
            double maxY = ((Number) max.get(1)).doubleValue();
            track.setBoundingBox(new Envelope(minX, maxX, minY, maxY));
        }
        save(track);
    }

    void removeUser(MongoUser user) {
        UpdateResults<MongoTrack> result =
                update(q().field(MongoTrack.USER).equal(key(user)), up().unset(MongoTrack.USER));
        if (result.getHadError()) {
            log.error("Error removing user {} from tracks: {}", user, result.getError());
        } else {
            log.debug("Removed user {} from {} tracks", user, result.getUpdatedCount());
        }
    }

    @Override
    protected Tracks createPaginatedIterable(Iterable<MongoTrack> i, Pagination p, long count) {
        return Tracks.from(i).withPagination(p).withElements(count).build();
    }

    @Override
    protected Iterable<MongoTrack> fetch(Query<MongoTrack> q) {
        return super.fetch(q.order(MongoTrack.RECENTLY_MODIFIED_ORDER));
    }

    @Override
    protected Tracks fetch(Query<MongoTrack> q, Pagination p) {
        return super.fetch(q.order(MongoTrack.RECENTLY_MODIFIED_ORDER), p);
    }

    protected <T> List<Object> toIdList(List<Key<T>> keys) {
        List<Object> ids = Lists.newArrayListWithExpectedSize(keys.size());
        for (Key<T> key : keys) {
            ids.add(key.getId());
        }
        return ids;
    }

    protected List<Object> toIdList(Collection<String> identifiers) {
        List<Object> ids = Lists.newArrayListWithExpectedSize(identifiers.size());
        for (String identifier : identifiers) {
            ids.add(new ObjectId(identifier));
        }
        return ids;
    }
    
    public Collection<String> getTrackIds(TrackMeasurementFilter request) {
       return measurementDao.getTrackIds(request);
    }
    
    @Override
    public List<Geometry> getGeometries(Track track) {
        return measurementDao.getGeometries(track);
       
    }

    @Override
    public Map<String, List<Geometry>> getGeometries(Collection<String> trackIds) {
        return measurementDao.getGeometries(trackIds);
    }
    
    public Map<String, TimeExtrema> getOfferingTimeExtrema() {
        AggregationOutput aggregate = aggregate(group(false));
        Map<String, TimeExtrema> ote = Maps.newHashMap();
        for (DBObject r : aggregate.results()) {
            TimeExtrema te = parseTimeExtrema(r);
            ote.put(r.get(Mapper.ID_KEY).toString(), te);
        }
        return ote;
    }
    
    public TimeExtrema getGlobalTimeExtrema() {
        AggregationOutput aggregate = aggregate(group(true));
        TimeExtrema te = null;
        for (DBObject r : aggregate.results()) {
            te = parseTimeExtrema(r);
        }
        return te;
    }
    
    private TimeExtrema parseTimeExtrema(DBObject r) {
        TimeExtrema te = new TimeExtrema();
        te.setMinPhenomenonTime(new DateTime(r.get(PHEN_START_TIME)));
        te.setMaxPhenomenonTime(new DateTime(r.get(PHEN_END_TIME)));
        te.setMinResultTime(new DateTime(r.get(RESULT_START_TIME)));
        te.setMaxResultTime(new DateTime(r.get(RESULT_END_TIME)));
        return te;
    }

    private DBObject group(boolean globale) {
        BasicDBObject fields = new BasicDBObject();
        if (globale) {
            fields.put(Mapper.ID_KEY, 0);
        } else {
            fields.put(Mapper.ID_KEY, MongoUtils.valueOf(MongoMeasurement.SENSOR, MongoSensor.NAME));
        }
        fields.put(PHEN_START_TIME, MongoUtils.min(MongoUtils.valueOf(MongoTrack.BEGIN)));
        fields.put(PHEN_END_TIME, MongoUtils.max(MongoUtils.valueOf(MongoTrack.END)));
        fields.put(RESULT_START_TIME, MongoUtils.min(MongoUtils.valueOf(MongoTrack.LAST_MODIFIED)));
        fields.put(RESULT_END_TIME, MongoUtils.max(MongoUtils.valueOf(MongoTrack.LAST_MODIFIED)));
        return MongoUtils.group(fields);
    }

    public class TimeExtrema {
        private DateTime minPhenomenonTime;

        private DateTime maxPhenomenonTime;

        private DateTime minResultTime;

        private DateTime maxResultTime;

        public DateTime getMinPhenomenonTime() {
            return minPhenomenonTime;
        }

        public void setMinPhenomenonTime(DateTime minPhenomenonTime) {
            this.minPhenomenonTime = minPhenomenonTime;
        }

        public DateTime getMaxPhenomenonTime() {
            return maxPhenomenonTime;
        }

        public void setMaxPhenomenonTime(DateTime maxPhenomenonTime) {
            this.maxPhenomenonTime = maxPhenomenonTime;
        }

        public DateTime getMinResultTime() {
            return minResultTime;
        }

        public void setMinResultTime(DateTime minResultTime) {
            this.minResultTime = minResultTime;
        }

        public DateTime getMaxResultTime() {
            return maxResultTime;
        }

        public void setMaxResultTime(DateTime maxResultTime) {
            this.maxResultTime = maxResultTime;
        }
    }

    @Override
    protected AggregationOutput aggregate(DBObject firstOp, DBObject... additionalOps) {
        return aggregate(MongoTrack.class, firstOp, additionalOps);
    }
}
