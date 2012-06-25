var crypto = require('crypto'),
    path = require('path'),
    util = require('util'),
    fs = require('fs'),
    http = require('http'),
    url = require('url'),
    qs = require('querystring'),
    child_proc = require('child_process');
    redis = require('redis');

var ALL_MV = 'all_movies',
    AVAIL_MV = 'available_movies',
    REMOVED_MV = 'removed_movies';

var IMDB_API = "www.imdbapi.com";

exports.connect = function(host, port, options) {
    this.client = redis.createClient(host, port, options);
}

exports.downloadPoster = function(hash, imageUrl, savepath) {
    var urlObj = url.parse(imageUrl);
    var client = this.client;
    http.get({'host': urlObj.host, 'path': urlObj.path}, function(res) {
        var wstream = res.pipe(fs.createWriteStream(savepath));
        wstream.on('close', function() {
            client.hset(hash, 'poster_image', path.basename(savepath));
        });
    });
}

exports.updateContentInfo = function(hash, force) {
    var client = this.client;
    client.hget(hash, 'content', function(err, reply) {
        if (err) {
            console.warn('fail to get "content" for ' + hash + ', err=' + util.inspect(err));
        } else {
            if (force || reply == null) {
                loadContentInfo(client, hash);
            } else {
                console.info('skipping loading content for ' + hash + ' as it is already there!');
            }   
        }   
    });
}

function loadContentInfo(client, hash) {
    client.hget(hash, 'fullpath', function(err, fullpath) {
        if (err) {
            console.warn('fail to get fullpath for ' + hash + ', err=' + util.inspect(err));
        } else {
            var cmd = '/usr/bin/du -sk *; /usr/bin/du -sk .';
            var options = {'cwd': fullpath};
            child_proc.exec(cmd, options, function(err, stdout, stderr) {
                if (err != null) {
                    console.warn('fail to run du for ' + fullpath + ', err=' + util.inspect(err));
                } else {
                    var content = to_content_obj(stdout.toString());
                    client.hset(hash, 'content', JSON.stringify(content));
                }
            });
        }
    });
}

function to_content_obj(output) {
    var lines = output.split('\n');
    var content = [];
    var totalSize = 0;
    for (var i = 0; i < lines.length; i++) {
        var line = lines[i].trim();
        if (line && line.length && line.length > 0) {
            var info = line.split('\t');
            var size = info[0];
            var file = info[1];
            if (file == '.') {
                totalSize = size;
            } else {
                content[content.length] = {'file': file, 'size': size};
            }
        }
    }
    return {'content': content, 'size': totalSize};
}

exports.updateIMDBInfo = function(hash, title, image_save_root, force) {
    var client = this.client;
    client.hget(hash, 'imdb', function(err, reply) {
        if (err) {
            console.warn('fail to get "imdb" for ' + hash + ', err=' + util.inspect(err));
        } else {
            if (force || reply == null) {
                loadIMDBInfo(client, hash, title, image_save_root);
            } else {
                console.info('skipping loading imdb for ' + hash + ' as it is already there!');
            }
        }
    });
}

function loadIMDBInfo(client, hash, title, image_save_root) {
    var options = {
        host: 'www.imdbapi.com',
        path: '/?' + qs.stringify({'t': title})
    };
    http.get(options, function(res) {
        if (res.statusCode == 200) {
            res.setEncoding('utf8');
            res.on('data', function(chunk) {
                var data = JSON.parse(chunk);
                if (data 
                    && data.Response 
                    && data.Response.toString().toLowerCase() == 'true') {
                        client.hset(hash, 'imdb', JSON.stringify(data), function(err, reply) {
                            if (err) {
                            } else {
                                var poster = data['Poster'];
                                if (poster && typeof poster == 'string' 
                                    && poster.indexOf('http://') == 0) {
                                    exports.downloadPoster(hash, 
                                        poster, 
                                        path.join(image_save_root, hash + path.extname(poster)));
                                }
                            }
                        });
                    }
            });
        }
    }); 
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

exports.loadMovie = function(key, callback) {
    client.hgetall(key, function(err, movie) {
        if (callback) {
            var reply = null;
            if (err) {
                reply = {'success': false, 'error': err, 'movie': {}};
            } else {
                reply = {'success': false, 'movie': to_frontend_movie(movie)};
            }
            callback(reply);
        }
    });
}

exports.listMovies = function(key, callback) {
    var client = this.client;
    client.smembers(key, function(err, keys) {
        if (err) {
        } else {
            if (keys && util.isArray(keys) && keys.length > 0) {
                var multi = client.multi();
                for (var i = 0; i < keys.length; i++) {
                    //multi.hmget(keys[i], _movie_props_for_frontend);
                    multi.hgetall(keys[i]);
                }
                multi.exec(function(err, movies) {
                    if (callback) {
                        var reply = null;
                        if (err) {
                            reply = {'success': false, 'error': err, 'movies': []};
                        } else {
                            movies = to_frontend_movies(movies);
                            movies.sort(function(a, b) {
                                return a.filename.localeCompare(b.filename);
                            });
                            reply = {'success': true, 'movies': movies};
                        }
                        callback(reply);
                    }
                });
            }
        }
    });
}

exports.addMovie = function(filepath) {
    var m = createMovieObj(filepath, true);
    if (m) {
        var key = m.hash;
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
    var m = createMovieObj(filepath, false);
    if (m) {
        var key = m.hash;
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
        var m = createMovieObj(filepath, true);
        var key = m.hash;
        keys[keys.length] = key;
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
    var filename = path.basename(filepath);
    var m = {};
    m['hash'] = getHash(filename);
    m['fullpath'] = filepath;
    m['filename'] = filename;
    m['available'] = avail;
    return m;
}

//var _movie_props_for_frontend = ['hash', 'filename', 'available'];
function to_frontend_movies(movies) {
    if (movies && movies.length && movies.length > 0) {
        for (var i = 0; i < movies.length; i++) {
            movies[i] = to_frontend_movie(movies[i]);
        }
        return movies;
    } else {
        return [];
    }
}

function to_frontend_movie(m) {
    delete m['fullpath'];
    if (m['content']) {
        m['content'] = JSON.parse(m['content']);
    }
    if (m['imdb']) {
        m['imdb'] = JSON.parse(m['imdb']);
    }
    return m;
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
