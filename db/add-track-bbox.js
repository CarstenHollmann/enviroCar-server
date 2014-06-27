function getMinMax(track) {
    var c = db.measurements.aggregate(
        { "$match": { "track": DBRef("tracks", track._id) } },
        {
            "$group": {
                "_id": 0,
                "min": { "$min": "$geometry.coordinates" },
                "max": { "$max": "$geometry.coordinates" },
            }
        }
    )
    return c.hasNext() ? c.next() : null;
}

db.tracks.find().forEach(function(track) {
    var bbox = getMinMax(track);
    if (bbox) {
        track.bbox = [
            bbox.min[0],
            bbox.min[1],
            bbox.max[0],
            bbox.max[1]
        ];
        db.tracks.save(track)
    }
});

db.measurements.ensureIndex({ "sensor._id" : -1 },{ "name" : "sensor._id_" });
