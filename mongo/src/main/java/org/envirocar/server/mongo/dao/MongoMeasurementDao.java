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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.BSONObject;
import org.bson.types.ObjectId;
import org.envirocar.server.core.TemporalFilter;
import org.envirocar.server.core.dao.MeasurementDao;
import org.envirocar.server.core.entities.Measurement;
import org.envirocar.server.core.entities.Measurements;
import org.envirocar.server.core.entities.Track;
import org.envirocar.server.core.entities.User;
import org.envirocar.server.core.exception.GeometryConverterException;
import org.envirocar.server.core.filter.MeasurementFilter;
import org.envirocar.server.core.filter.TrackMeasurementFilter;
import org.envirocar.server.core.util.GeometryConverter;
import org.envirocar.server.core.util.Pagination;
import org.envirocar.server.mongo.MongoDB;
import org.envirocar.server.mongo.entity.MongoMeasurement;
import org.envirocar.server.mongo.entity.MongoSensor;
import org.envirocar.server.mongo.entity.MongoTrack;
import org.envirocar.server.mongo.entity.MongoUser;
import org.envirocar.server.mongo.util.MongoUtils;
import org.envirocar.server.mongo.util.MorphiaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jmkgreen.morphia.Datastore;
import com.github.jmkgreen.morphia.Key;
import com.github.jmkgreen.morphia.mapping.Mapper;
import com.github.jmkgreen.morphia.query.MorphiaIterator;
import com.github.jmkgreen.morphia.query.Query;
import com.github.jmkgreen.morphia.query.QueryImpl;
import com.github.jmkgreen.morphia.query.UpdateResults;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.vividsolutions.jts.geom.Geometry;

/**
 * TODO JavaDoc
 *
 * @author Arne de Wall
 */
public class MongoMeasurementDao extends AbstractMongoDao<ObjectId, MongoMeasurement, Measurements> implements
        MeasurementDao {
    public static final String ID = Mapper.ID_KEY;

    private static final Logger log = LoggerFactory.getLogger(MongoMeasurementDao.class);

    private static final String TRACKS = "tracks";

    private static final String TRACK_NAME_PATH = MongoUtils.path(TRACKS, MongoMeasurement.TRACK,
            MongoMeasurement.IDENTIFIER);

    private static final String TRACK_NAME_VALUE = MongoUtils.valueOf(TRACK_NAME_PATH);

    public static final String TRACK_VALUE = MongoUtils.valueOf(MongoMeasurement.TRACK);

    private static final String GEOMETRIES = "geometries";

    private final GeometryConverter<BSONObject> geometryConverter;
    
    @Inject
    private MongoTrackDao trackDao;

    @Inject
    protected MongoMeasurementDao(MongoDB mongoDB, GeometryConverter<BSONObject> geometryConverter) {
        super(MongoMeasurement.class, mongoDB);
        this.geometryConverter = geometryConverter;
    }

    @Override
    public MongoMeasurement create(Measurement measurement) {
        return save(measurement);
    }

    @Override
    public MongoMeasurement save(Measurement measurement) {
        MongoMeasurement mongoMeasurement = (MongoMeasurement) measurement;
        save(mongoMeasurement);
        return mongoMeasurement;
    }

    @Override
    public void delete(Measurement m) {
        delete(((MongoMeasurement) m).getId());
        if (m.hasTrack()) {
            updateTrackTimeForDeletedMeasurement(m);
            trackDao.calculateBoundingBox(m.getTrack());
        }
    }

    public void updateTrackTimeForDeletedMeasurement(Measurement m) {
        boolean update = false;
        Track track = m.getTrack();
        if (track.hasBegin() && m.getTime().equals(track.getBegin())) {
            MongoMeasurement newBegin =
                    q().field(MongoMeasurement.TRACK).equal(key(track)).order(MongoMeasurement.TIME).limit(1).get();
            track.setBegin(newBegin == null ? null : newBegin.getTime());
            update = true;
        }
        if (track.hasEnd() && m.getTime().equals(track.getEnd())) {
            MongoMeasurement newEnd =
                    q().field(MongoMeasurement.TRACK).equal(key(track))
                            .order(MongoUtils.reverse(MongoMeasurement.TIME)).limit(1).get();
            track.setEnd(newEnd == null ? null : newEnd.getTime());
            update = true;
        }
        if (update) {
            trackDao.save(track);
        }
    }

    @Override
    public Measurements get(MeasurementFilter request) {
        if (request.hasGeometry()) {
            // needed because of lacking geo2d support in morphia
            return getMongo(request);
        } else {
            return getMorphia(request);
        }
    }

    private Measurements getMorphia(MeasurementFilter request) {
        Query<MongoMeasurement> q = q().order(MongoMeasurement.TIME);
        if (request.hasTrack()) {
            q.field(MongoMeasurement.TRACK).equal(key(request.getTrack()));
        }
        if (request.hasUser()) {
            q.field(MongoMeasurement.USER).equal(key(request.getUser()));
        }
        if (request.hasTemporalFilter()) {
                MorphiaUtils.temporalFilter(q.field(MongoMeasurement.TIME), request.getTemporalFilter());
        }
        if (request.hasSensors()) {
            q.field(MongoMeasurement.SENSOR).in(request.getSensors());
        }
        if (request.hasPhenomeon()) {
            q.field(MongoMeasurement.PHENOMENONS).in(request.getPhenomenon());
        }
        // ids
        if (request.hasSensorIds()) {
            q.field(MongoUtils.path(MongoMeasurement.SENSOR, Mapper.ID_KEY)).in(toObjectIds(request.getSensorIds()));
        }
        if (request.hasPhenomenonIds()) {
            q.field(MongoUtils.path(MongoMeasurement.PHENOMENONS, MongoMeasurement.PHEN, Mapper.ID_KEY)).in(request.getPhenomenonIds());
        }
        if (request.hasTrackIds()) {
            BasicDBList l = new BasicDBList();
            for (String string : request.getTrackIds()) {
                MongoTrack track = new MongoTrack();
                track.setIdentifier(string);
                l.add(ref(track));
            }
            q.field(MongoMeasurement.TRACK).in(l);
        }
        return fetch(q, request.getPagination());
    }

    private Measurements getMongo(MeasurementFilter request) {
        BasicDBObjectBuilder q = bson();
        if (request.hasGeometry()) {
            q.add(MongoMeasurement.GEOMETRY, withinGeometry(request.getGeometry()));
        }
        if (request.hasTrack()) {
            q.add(MongoMeasurement.TRACK, ref(request.getTrack()));
        }
        if (request.hasUser()) {
            q.add(MongoMeasurement.USER, ref(request.getUser()));
        }
        if (request.hasTemporalFilter()) {
                q.add(MongoMeasurement.TIME, MongoUtils.temporalFilter(request.getTemporalFilter()));
        }
        // ids
        if (request.hasSensorIds()) {
            BasicDBList l = new BasicDBList();
            for (String s : request.getSensorIds()) {
//                MongoSensor s = new MongoSensor();
//                s.setIdentifier(string);
                l.add(new ObjectId(s));
            }
            q.add(MongoUtils.path(MongoMeasurement.SENSOR, Mapper.ID_KEY), MongoUtils.in(l));
        }
        if (request.hasPhenomenonIds()) {
            BasicDBList l = new BasicDBList();
            for (String p : request.getPhenomenonIds()) {
//                MongoPhenomenon p = new MongoPhenomenon();
//                p.setName(string);
                l.add(p);
            }
            q.add(MongoUtils.path(MongoMeasurement.PHENOMENONS, MongoMeasurement.PHEN, Mapper.ID_KEY), MongoUtils.in(l));
        }
        if (request.hasTrackIds()) {
            BasicDBList l = new BasicDBList();
            for (String string : request.getTrackIds()) {
                MongoTrack track = new MongoTrack();
                track.setIdentifier(string);
                l.add(ref(track));
            }
            q.add(MongoMeasurement.TRACK, MongoUtils.in(l));
        }
        return query(q.get(), request.getPagination());
    }

    @Override
    public Measurement getById(String id) {
        ObjectId oid;
        try {
            oid = new ObjectId(id);
        } catch (IllegalArgumentException e) {
            return null;
        }
        return get(oid);
    }

    void removeUser(MongoUser user) {
        UpdateResults<MongoMeasurement> result =
                update(q().field(MongoMeasurement.USER).equal(key(user)), up().unset(MongoMeasurement.USER));
        if (result.getHadError()) {
            log.error("Error removing user {} from measurement: {}", user, result.getError());
        } else {
            log.debug("Removed user {} from {} measurements", user, result.getUpdatedCount());
        }
    }

    @Override
    protected Measurements createPaginatedIterable(Iterable<MongoMeasurement> i, Pagination p, long count) {
        return Measurements.from(i).withPagination(p).withElements(count).build();
    }

    void removeTrack(MongoTrack track) {
        UpdateResults<MongoMeasurement> result =
                update(q().field(MongoMeasurement.TRACK).equal(key(track)), up().unset(MongoMeasurement.TRACK));
        if (result.getHadError()) {
            log.error("Error removing track {} from measurements: {}", track, result.getError());
        } else {
            log.debug("Removed track {} from {} measurements", track, result.getUpdatedCount());
        }
    }

    List<Key<MongoTrack>> getTrackKeysByBbox(MeasurementFilter filter) {
        ArrayList<DBObject> filters = new ArrayList<DBObject>(4);
        if (filter.hasGeometry()) {
            filters.add(matchGeometry(filter.getGeometry()));
        }
        if (filter.hasUser()) {
            filters.add(matchUser(filter.getUser()));
        }
        if (filter.hasTrack()) {
            filters.add(matchTrack(filter.getTrack()));
        }
        if (filter.hasTemporalFilter()) {
            filters.add(matchTime(filter.getTemporalFilter()));
        }

        final AggregationOutput out;
        if (filters.isEmpty()) {
            out = aggregate(project(), group());
        } else {
            int size = filters.size();
            if (size == 1) {
                out = aggregate(filters.get(0), project(), group());
            } else {
                DBObject first = filters.get(0);
                DBObject[] other = new DBObject[size + 1];
                for (int i = 1; i < size; ++i) {
                    other[i - 1] = filters.get(i);
                }
                other[other.length - 2] = project();
                other[other.length - 1] = group();
                out = aggregate(first, other);
            }
        }
        return toKeyList(out.results());
    }

    protected AggregationOutput aggregate(DBObject firstOp, DBObject... additionalOps) {
        return aggregate(MongoMeasurement.class, firstOp, additionalOps);
    }

    private DBObject matchGeometry(Geometry polygon) {
        return MongoUtils.match(MongoMeasurement.GEOMETRY, withinGeometry(polygon));
    }

    private DBObject matchUser(User user) {
        return MongoUtils.match(MongoMeasurement.USER, ref(user));
    }

    private DBObject matchTrack(Track track) {
        return MongoUtils.match(MongoMeasurement.TRACK, ref(track));
    }

    private DBObject matchTime(TemporalFilter tf) {
        return MongoUtils.match(MongoMeasurement.TIME, MongoUtils.temporalFilter(tf));
    }

    private DBObject withinGeometry(Geometry polygon) {
        try {
            return MongoUtils.geoWithin(geometryConverter.encode(polygon));
        } catch (GeometryConverterException e) {
            throw new RuntimeException(e);
        }
    }

    private DBObject project() {
        return MongoUtils.project(new BasicDBObject(MongoMeasurement.TRACK, 1));
    }

    private DBObject group() {
        BasicDBObject fields = new BasicDBObject();
        fields.put(ID, TRACK_NAME_VALUE);
        fields.put(TRACKS, MongoUtils.addToSet(TRACK_VALUE));
        return MongoUtils.group(fields);
    }

    protected List<Key<MongoTrack>> toKeyList(Iterable<DBObject> res) {
        List<Key<MongoTrack>> keys = Lists.newLinkedList();
        for (DBObject obj : res) {
            BasicDBList list = (BasicDBList) obj.get(TRACKS);
            for (Object p : list) {
                DBRef ref = (DBRef) p;
                Key<MongoTrack> key = getMapper().refToKey(ref);
                keys.add(key);
            }
        }
        return keys;
    }

    protected Collection<String> toIdList(Iterable<DBObject> res) {
        Set<String> ids = Sets.newLinkedHashSet();
        for (Key<MongoTrack> key : toKeyList(res)) {
            ids.add(key.getId().toString());
        }
        return ids;
    }

    private Measurements query(DBObject query, Pagination p) {
        final Mapper mapper = getMapper();
        final Datastore ds = getMongoDB().getDatastore();
        final DBCollection coll = ds.getCollection(MongoMeasurement.class);

        DBCursor cursor = coll.find(query, null);
        long count = 0;

        cursor.setDecoderFactory(ds.getDecoderFact());
        if (p != null) {
            count = coll.count(query);
            if (p.getOffset() > 0) {
                cursor.skip(p.getOffset());
            }
            if (p.getLimit() > 0) {
                cursor.limit(p.getLimit());
            }
        }
        cursor.sort(QueryImpl.parseFieldsString(MongoMeasurement.TIME, MongoMeasurement.class, mapper, true));
        Iterable<MongoMeasurement> i =
                new MorphiaIterator<MongoMeasurement, MongoMeasurement>(cursor, mapper, MongoMeasurement.class,
                        coll.getName(), mapper.createEntityCache());
        return createPaginatedIterable(i, p, count);
    }

    List<Geometry> getGeometries(Track track) {
        AggregationOutput aggregate = aggregate(matchTrack(track), sortTime(), groupTrackGeometries(true));
        List<Geometry> geoms = Lists.newArrayList();
        for (DBObject dbObject : aggregate.results()) {
            if (dbObject.containsField(GEOMETRIES)) {
                return parseGeometries((BasicDBList) dbObject.get(GEOMETRIES));
            }
        }
        return geoms;
    }

    Map<String, List<Geometry>> getGeometries(Collection<String> trackIds) {
        AggregationOutput aggregate = null;
        if (trackIds != null && !trackIds.isEmpty()) {
            aggregate = aggregate(matchTrackGeometries(trackIds), sortTime(), groupTrackGeometries(true));
        } else {
            aggregate = aggregate(sortTime(), groupTrackGeometries(true));
        }
        Map<String, List<Geometry>> map = Maps.newLinkedHashMap();
        for (DBObject dbObject : aggregate.results()) {
            String id = ((ObjectId) ((DBRef) dbObject.get(ID)).getId()).toString();
            if (dbObject.containsField(GEOMETRIES)) {
                map.put(id, parseGeometries((BasicDBList) dbObject.get(GEOMETRIES)));
            }
        }
        return map;
    }

    private List<Geometry> parseGeometries(BasicDBList basicDBList) {
        List<Geometry> geoms = Lists.newLinkedList();
        for (Object object : basicDBList) {
            try {
                geoms.add(geometryConverter.decodeGeometry((BSONObject) object));
            } catch (GeometryConverterException e) {
                log.error("Error while converting geometry", e);
            }
        }
        return geoms;
    }

    private DBObject matchTrackGeometries(Collection<String> foiIDs) {
        BasicDBList l = new BasicDBList();
        for (String string : foiIDs) {
            MongoTrack track = new MongoTrack();
            track.setIdentifier(string);
            l.add(ref(track));
        }
        BasicDBObject b = new BasicDBObject();
        b.put(MongoMeasurement.TRACK, MongoUtils.in(l));
        return MongoUtils.match(b);
    }

    private DBObject sortTime() {
        BasicDBObject sortFields = new BasicDBObject();
        sortFields.put(MongoMeasurement.TIME, 1);
        return MongoUtils.sort(sortFields);
    }

    private DBObject groupTrackGeometries(boolean withTrack) {
        BasicDBObject projectFields = new BasicDBObject();
        if (withTrack) {
            projectFields.put(ID, MongoUtils.valueOf(MongoMeasurement.TRACK));
        }
        projectFields.put(GEOMETRIES, MongoUtils.push(MongoUtils.valueOf(MongoMeasurement.GEOMETRY)));
        return MongoUtils.group(projectFields);
    }

    Iterable<DBObject> getDistinctPhenomenons() {
        AggregationOutput aggregate = aggregate(unwindPhenomenons(), groupPhenMap());
        return aggregate.results();
    }

    private DBObject groupPhenMap() {
        BasicDBObject fields = new BasicDBObject();
        fields.put(ID, MongoUtils.valueOf(MongoMeasurement.SENSOR, MongoSensor.NAME));
        fields.put(MongoMeasurement.PHENOMENONS,
                MongoUtils.addToSet(MongoMeasurement.PHENOMENONS, MongoMeasurement.PHEN, Mapper.ID_KEY));
        return MongoUtils.group(fields);
    }

    Iterable<DBObject> getDistinctPhenomenons(String sensorId) {
        AggregationOutput aggregate = aggregate(matchPhen(sensorId), unwindPhenomenons(), groupPhen());
        return aggregate.results();
    }

    private DBObject matchPhen(String sensorId) {
        return MongoUtils.match(MongoUtils.path(MongoMeasurement.SENSOR, MongoSensor.NAME), new ObjectId(sensorId));
    }

    private DBObject unwindPhenomenons() {
        return MongoUtils.unwind(MongoUtils.valueOf(MongoMeasurement.PHENOMENONS));
    }

    private DBObject groupPhen() {
        BasicDBObject fields = new BasicDBObject();
        fields.put(ID, MongoUtils.valueOf(MongoMeasurement.PHENOMENONS, MongoMeasurement.PHEN, Mapper.ID_KEY));
        return MongoUtils.group(fields);
    }

//    public Iterable<DBObject> getDistinctSensors(String phenomenonId) {
//        AggregationOutput aggregate = aggregate(matchSensor(phenomenonId), groupSensor());
//        return aggregate.results();
//    }
//
//    private DBObject matchSensor(String phenomenonId) {
//        // BasicDBObjectBuilder b = new BasicDBObjectBuilder();
//        // BasicDBObjectBuilder match = b.push(Ops.MATCH);
//        // match.add(MongoUtils.path(MongoMeasurement.PHENOMENONS,
//        // MongoMeasurement.PHEN, Mapper.ID_KEY), phenomenonId);
//        // return b.get();
//        return MongoUtils.match(MongoUtils.path(MongoMeasurement.PHENOMENONS, MongoMeasurement.PHEN, Mapper.ID_KEY),
//                phenomenonId);
//    }
//
//    private DBObject groupSensor() {
//        BasicDBObject fields = new BasicDBObject();
//        fields.put(ID, MongoUtils.valueOf(MongoMeasurement.SENSOR, MongoSensor.NAME));
//        return MongoUtils.group(fields);
//    }

    Iterable<DBObject> getDistinctSensors() {
        AggregationOutput aggregate = aggregate(unwindPhenomenons(), groupPhenSensors());
        return aggregate.results();
    }

    private DBObject groupPhenSensors() {
        BasicDBObject fields = new BasicDBObject();
        fields.put(ID, MongoUtils.valueOf(MongoMeasurement.PHENOMENONS, MongoMeasurement.PHEN, Mapper.ID_KEY));
        fields.put(MongoMeasurement.SENSORS, MongoUtils.addToSet(MongoMeasurement.SENSOR, MongoSensor.NAME));
        return MongoUtils.group(fields);
    }

    Collection<String> getTrackIds(TrackMeasurementFilter request) {
        ArrayList<DBObject> filters = new ArrayList<DBObject>(4);
        if (request.hasGeometries()) {
            filters.add(matchGeometries(request.getGeometries()));
        }
        if (request.hasSensorIds()) {
            filters.add(matchSensorIds(request.getSensorIds()));
        }
        if (request.hasPhenomenonIds()) {
            filters.add(matchPhenomenonIds(request.getPhenomenonIds()));
        }

        final AggregationOutput out;
        if (filters.isEmpty()) {
            out = aggregate(projectTrackIds(), groupTrackIds());
        } else {
            int size = filters.size();
            if (size == 1) {
                out = aggregate(filters.get(0), projectTrackIds(), groupTrackIds());
            } else {
                DBObject first = filters.get(0);
                DBObject[] other = new DBObject[size + 1];
                for (int i = 1; i < size; ++i) {
                    other[i - 1] = filters.get(i);
                }
                other[other.length - 2] = projectTrackIds();
                other[other.length - 1] = groupTrackIds();
                out = aggregate(first, other);
            }
        }
        return toIdList(out.results());
    }

    private DBObject projectTrackIds() {
        return MongoUtils.project(new BasicDBObject(MongoMeasurement.TRACK, 1));
    }

    private DBObject groupTrackIds() {
        BasicDBObject fields = new BasicDBObject();
        fields.put(ID, TRACK_NAME_VALUE);
        fields.put(TRACKS, MongoUtils.addToSet(TRACK_VALUE));
        return MongoUtils.group(fields);
    }

    private DBObject matchGeometries(Collection<Geometry> geometries) {
        try {
            List<DBObject> objects = Lists.newArrayListWithCapacity(geometries.size());
            for (Geometry geometry : geometries) {
                objects.add(MongoUtils.geoWithin(geometryConverter.encode(geometry)));
            }
            return MongoUtils.match(MongoUtils.or(MongoMeasurement.GEOMETRY, objects));
        } catch (GeometryConverterException e) {
            throw new RuntimeException(e);
        }
    }

    private DBObject matchSensorIds(Collection<String> sensorIds) {
        return MongoUtils.match(MongoUtils.or(MongoUtils.path(MongoMeasurement.SENSOR, Mapper.ID_KEY),
                toObjectIds(sensorIds)));
    }

    private DBObject matchPhenomenonIds(Collection<String> phenomenonIds) {
        // MongoUtils.or(MongoUtils.path(MongoMeasurement.PHENOMENONS,
        // MongoMeasurement.PHEN, Mapper.ID_KEY), phenomenonIds);
        // return MongoUtils.match(MongoUtils.path(MongoMeasurement.PHENOMENONS,
        // MongoMeasurement.PHEN, Mapper.ID_KEY), MongoUtils.in(phenomenonIds));
        return MongoUtils.match(MongoUtils.or(
                MongoUtils.path(MongoMeasurement.PHENOMENONS, MongoMeasurement.PHEN, Mapper.ID_KEY), phenomenonIds));
    }
}
