db.tracks.find().forEach(function(track) {
    var first = true;
    var bbox = [];

    db.measurements.find({ track: DBRef("tracks", track._id)})
        .forEach(function(measurement) {
            if (!measurement.geometry || measurement.geometry.type !== "Point") {
                print("No Point geometry in measurement " + measurement._id);
                return;
            }
            var x = measurement.geometry.coordinates[0];
            var y = measurement.geometry.coordinates[1];

            if (first) {
                first = false;
                bbox[0] = x;
                bbox[2] = x;
                bbox[1] = y;
                bbox[3] = y;
            } else {
                if (x < bbox[0]) { bbox[0] = x; }
                if (x > bbox[2]) { bbox[2] = x; }

                if (y < bbox[1]) { bbox[1] = y; }
                if (y > bbox[3]) { bbox[3] = y; }
            }
    });
    track.bbox = bbox;
    db.tracks.save(track);
});