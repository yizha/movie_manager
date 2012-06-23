var crypto = require('crypto'),
    path = require('path'),
    util = require('util'),
    fs = require('fs'),
    redis = require('redis');

var ALL_MV = 'all_movies',
    AVAIL_MV = 'available_movies',
    REMOVED_MV = 'removed_movies';


exports.connect = function(host, port, options) {
    this.client = redis.createClient(host, port, options);
}

exports.listAllMovies = function(callback) {
    exports.listMovies(ALL_MV, callback);
}

exports.listAvailableMovies = function(callback) {
    exports.listMovies(AVAIL_MV, callback);
}

exports.listRemovedMovies = function(callback) {
    exports.listMovies(REMOVED_MV, callback);
}

exports.listMovies = function(key, callback) {
    var client = this.client;
    client.smembers(key, function(err, keys) {
        if (err) {
        } else {
            if (keys && util.isArray(keys) && keys.length > 0) {
                var multi = client.multi();
                for (var i = 0; i < keys.length; i++) {
                    multi.hmget(keys[i], _movie_props_for_frontend);
                }
                multi.exec(function(err, data) {
                    var reply = null;
                    if (err) {
                        reply = {'success': false, 'error': err, 'movies': []};
                    } else {
                        var movies = to_frontend_movies_from_hmget(data);
                        movies.sort(function(a, b) {
                            return a.filename.localeCompare(b.filename);
                        });
                        reply = {'success': true, 'movies': movies};
                    }
                    if (callback) callback(reply);
                });
            }
        }
    });
}

exports.addMovie = function(filepath) {
    var key = getHash(filepath);
    var m = createMovieObj(filepath, true);
    if (m) {
        var client = this.client;
        client.hmset(key, m, function(err, reply) {
            if (err == null && reply.toString().toUpperCase() == 'OK') {
                client.multi([
                    ['sadd', ALL_MV, key],
                    ['sadd', AVAIL_MV, key],
                    ['srem', REMOVED_MV, key]
                ]).exec(function(err, replies) {});
            }
        });
    }
}

exports.removeMovie = function(filepath) {
    var key = getHash(filepath);
    var m = createMovieObj(filepath, false);
    if (m) {
        var client = this.client;
        client.hmset(key, m, function(err, reply) {
            if (err == null && reply.toString().toUpperCase() == 'OK') {
                client.multi([
                    ['sadd', REMOVED_MV, key],
                    ['srem', AVAIL_MV, key]
                ]).exec(function(err, replies) {});
            }   
        }); 
    }
}

/*exports.addMovies = function(filepaths) {
    if (filepaths && util.isArray(filepaths) && filepaths.length > 0) {
        var client = this.client;
        for (var i = 0; i < filepaths.length; i++) {
            exports.addMovie(client, filepaths[i]);
        }
    }
}*/

exports.loadMovieFromDir = function(dir) {
    var client = this.client;
    var files = listDirs(dir);
    var multi = client.multi();
    var keys = [];
    for (var i = 0; i < files.length; i++) {
        var filepath = files[i];
        var key = getHash(filepath);
        keys[keys.length] = key;
        var m = createMovieObj(filepath, true);
        multi.hmset(key, m);
    }
    multi.exec(function(err, replies) {
        if (err) {
        } else {
            var multi2 = client.multi();
            multi2.del(AVAIL_MV);
            if (keys.length > 0) {
                multi2.sadd(ALL_MV, keys);
                multi2.sadd(AVAIL_MV, keys);
            }
            multi2.sdiffstore(REMOVED_MV, ALL_MV, AVAIL_MV);
            multi2.exec(function(err, replies) {
                if (err) {
                } else {
                    client.smembers(REMOVED_MV, function(err, reply) {
                        if (err) {
                        } else if (reply && util.isArray(reply) && reply.length > 0) {
                            var multi3 = client.multi();
                            for (var i = 0; i < reply.length; i++) {
                                multi3.hset(reply[i], 'available', 'false');
                            }
                            multi3.exec(function(err, replies){});
                        }
                    });
                }
            });
        }
    });
}

function listDir(dir) {
    if (path.existsSync(dir)) {
        files = fs.readdirSync(dir);
        if (files && files.length && files.length > 0) {
            for (var i = 0; i < files.length; i++) {
                files[i] = path.join(dir, files[i]);
            }
            return files;
        } else {
            return [];
        }
    } else {
        return [];
    }
}

function listDirs(dirs) {
    if (dirs) {
        var files = null;
        if (util.isArray(dirs) && dirs.length > 0) {
            files = [];
            for (var i = 0; i < dirs.length; i++) {
                files = files.concat(listDir(dirs[i]));
            }
            return files;
        } else {
            return listDir(dirs);
        }
    }
}

function getHash(Str) {
    return crypto.createHash('sha1')
                 .update(Str, 'utf8')
                 .digest('hex');
}

function createMovieObj(filepath, avail) {
    var m = {};
    m['hash'] = getHash(filepath);
    m['fullpath'] = filepath;
    m['filename'] = path.basename(filepath);
    m['available'] = avail;
    return m;
}

var _movie_props_for_frontend = ['hash', 'filename', 'available'];
function to_frontend_movies_from_hmget(data) {
    if (data && data.length && data.length > 0) {
        var movies = [];
        for (var i = 0; i < data.length; i++) {
            movies[movies.length] = {'hash':data[i][0], 'filename':data[i][1], 'available':data[i][2]};
        }
        return movies;
    } else {
        return [];
    }
}

var monitor = function(dir) {
    if (!path.existsSync(dir)) {
        console.warn('monitor target "' + dir + '" does not exist!');
        return;
    }
    fs.watch(dir, function(event, filename) {
        if (event == "rename") {
            if (filename) {
                if (on_rename) {
                    on_rename(path.join(dir, filename));
                }
            }
        } else if (event == "change") {
            if (filename) {
                if (on_change) {
                    on_change(path.join(dir, filename));
                }
            }
        }
    });
};

function on_rename(filepath) {
    path.exists(filepath, function(exists) {
        if (exists) {
            //console.log('add file: ' + filepath);
            exports.addMovie(filepath);
        } else {
            //console.log('remove file: ' + filepath);
            exports.removeMovie(filepath);
        }
    });
};

function on_change(filepath) {};

exports.watch = function(dir) {
    if (dir && dir.length && dir.length > 0) {
        if (util.isArray(dir)) {
            for (var i = 0; i < dir.length; i++) {
                monitor(dir[i]);
            }
        } else {
            monitor(dir);
        }
    }
};
