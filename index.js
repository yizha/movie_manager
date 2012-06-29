var crypto = require('crypto'),
    path = require('path'),
    util = require('util'),
    fs = require('fs'),
    http = require('http'),
    url = require('url'),
    qs = require('querystring'),
    child_proc = require('child_process');
    redis = require('redis');

var ALL_MV = 'all',
    AVAIL_MV = 'available',
    REMOVED_MV = 'removed';

exports.connect = function(host, port, options) {
    this.client = redis.createClient(host, port, options);
}

function getMoviesKey(type) {
    return 'movies:' + type;
}

function getMovieKey(hash) {
    return 'movie:' + hash;
}

function getMovieUserKey(hash) {
    return 'movie:' + hash + ':user';
}

function getUserMovieKey(user) {
    return 'user:' + user + ':movie';
}

exports.markUserMovie = function(user, hash, mark, callback) {
    var client = this.client;
    var cmd = (mark.toLowerCase() == 'true' ? 'sadd' : 'srem');
    var multi = client.multi([
            [cmd, getMovieUserKey(hash), user],
            [cmd, getUserMovieKey(user), hash]
        ]).exec(function(err, reply) {
            if (callback) {
                if (err) {
                    reply = {'success': false, 'error': err};
                } else {
                    reply = {'success': true};
                }
                callback(reply);
            }
        });
}

exports.allUsers = function(callback) {
    var client = this.client;
    client.keys('user:*', function(err, users) {
        if (callback) {
            if (err) {
                reply = {'success': false, 'error': err, 'users': []};
            } else {
                var usernames = [];
                if (users && users instanceof Array && users.length > 0) {
                    for (var i = 0; i < users.length; i++) {
                        // user:dingyc:movies
                        var user = users[i];
                        var start = user.indexOf(':');
                        var end = user.lastIndexOf(':');
                        usernames[usernames.length] = user.substring(start + 1, end);
                    }
                }
                reply = {'success': true, 'users': usernames};
            }
            callback(reply);
        }
    });
}

exports.loadMovieFilenames = function(hashes, callback) {
    var client = this.client;
    var multi = client.multi();
    for (var i = 0; i < hashes.length; i++) {
        multi.hget(getMovieKey(hashes[i]), 'filename');
    }
    multi.exec(function(err, filenames) {
        if (callback) {
            if (err) {
                reply = {'success': false, 'error': err, 'filenames': []};
            } else {
                reply = {'success': true, 'filenames': filenames};
            }
            callback(reply);
        }
    });
}

exports.loadUserMovies = function(user, callback) {
    var client = this.client;
    client.smembers(getUserMovieKey(user), function(err, hashes) {
        if (callback) {
            if (err) {
                reply = {'success': false, 'error': err, 'hashes': []};
            } else {
                reply = {'success': true, 'hashes': hashes};
            }
            callback(reply);
        }
    });
}

exports.loadMovieUsers = function(hash, callback) {
    var client = this.client;
    var multi = client.multi();
    multi.hget(getMovieKey(hash), 'filename');
    multi.smembers(getMovieUserKey(hash));
    multi.exec(function(err, reply) {
        if (callback) {
            if (err) {
                reply = {'success': false, 'error': err, 'filename': null, users: []};
            } else {
                reply = {'success': true, 'filename':reply[0], 'users': reply[1]};
            }
            callback(reply);
        }
    });
}

exports.loadAllUserMovies = function(callback) {
    if (callback && callback instanceof Function) {
        exports.loadMarkedMovies(function(data) {
            if (data.success) {
                var result = {};
                var movies = data.movies;
                for (var i = 0; i < movies.length; i++) {
                    var m = movies[i];
                    var users = m.users;
                    for (var j = 0; j < users.length; j++) {
                        var movieArray = null;
                        if (users[j] in result) {
                            movieArray = result[users[j]];
                        } else {
                            movieArray = [];
                            result[users[j]] = movieArray;
                        }
                        movieArray[movieArray.length] = m.filename;
                    }
                }
                callback({'success': true, 'all_user_movies': result});
            } else {
                callback({'success': false, 'error': data.error, 'all_user_movies': {}});
            }
        });
    }
}

exports.loadMarkedMovies = function(callback) {
    if (callback && callback instanceof Function) {
        var client = this.client;
        client.keys('movie:*:user', function(err, keys) {
            if (err) {
                callback({'success': false, 'error': err, 'movies': []});
            } else {
                if (keys && keys instanceof Array && keys.length > 0) {
                    var multi = client.multi();
                    for (var i = 0; i < keys.length; i++) {
                        var idx = keys[i].lastIndexOf(':');
                        var movieKey = keys[i].substring(0, idx);
                        multi.hget(movieKey, 'filename');
                        multi.smembers(keys[i]);
                    }
                    multi.exec(function(e, r) {
                        if (e) {
                            callback({'success': false, 'error': e, 'movies': []});
                        } else {
                            var data = [];
                            if (r && r instanceof Array && r.length > 0) {
                                for (var i = 0; i < r.length; i = i + 2) {
                                    var filename = r[i];
                                    var users = r[i + 1];
                                    data[data.length] = {'filename': filename, 'users': users};
                                }
                            }
                            callback({'success': true, 'movies': data});
                        }
                    });
                }
            }
        });
    }
}

exports.removeField = function(hash, field, callback) {
    var client = this.client;
    var key = getMovieKey(hash);
    client.hdel(key, field, function(err, reply) {
        if (callback) {
            if (err) {
                reply = {'success': false, 'error': err};
            } else {
                reply = {'success': true};
            }
            callback(reply);
        }
    });
}

exports.setField = function(hash, field, value, callback) {
    var client = this.client;
    var key = getMovieKey(hash);
    client.hset(key, field, value, function(err, reply) {
        if (callback) {
            var reply = null;
            if (err) {
                reply = {'success': false, 'error': err};
            } else {
                reply = {'success': true};
            }   
            callback(reply);
        }
    });
}

exports.downloadPoster = function(hash, imageUrl, savepath, callback) {
    var urlObj = url.parse(imageUrl);
    var client = this.client;
    http.get({'host': urlObj.host, 'path': urlObj.path}, function(res) {
        var wstream = res.pipe(fs.createWriteStream(savepath));
        wstream.on('close', function() {
            var key = getMovieKey(hash);
            client.hset(key, 'poster_image', imageUrl, function(err, reply) {
                if (callback) {
                    var reply = null;
                    if (err) {
                        reply = {'success': false, 'error': err};
                    } else {
                        reply = {'success': true};
                    }
                    callback(reply);
                }
            });
        });
    });
}

exports.loadFilesAndSize = function(hash, callback) {
    var client = this.client;
    var key = getMovieKey(hash);
    client.hget(key, 'fullpath', function(err, fullpath) {
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
                    var key = getMovieKey(hash);
                    client.hset(key, 'content', JSON.stringify(content), function(err, reply) {
                        if (callback) {
                            var reply = null;
                            if (err) {
                                reply = {'success': false, 'error': err};
                            } else {
                                reply = {'success': true};
                            }
                            callback(reply);
                        }    
                    });
                }
            });
        }
    });
}

function to_content_obj(output) {
    var lines = output.split('\n');
    var files = [];
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
                files[files.length] = {'file': file, 'size': size};
            }
        }
    }
    return {'files': files, 'size': totalSize};
}

function formatIMDBData(data) {
    data['url'] = 'http://www.imdb.com/title/' + data['imdbID'] + '/';
    data['id'] = data['imdbID'];
    data['Votes'] = data['imdbVotes'];
    data['Rating'] = data['imdbRating'];
    delete data['imdbID'];
    delete data['imdbVotes'];
    delete data['imdbRating'];
    delete data['Response'];
    return data;
}

exports.setIMDB = function(hash, qsObj, image_save_root, callback) {
    var client = this.client;
    var options = {
        host: 'www.imdbapi.com',
        path: '/?' + qs.stringify(qsObj)
    };
    http.get(options, function(res) {
        if (res.statusCode == 200) {
            res.setEncoding('utf8');
            var jsonStr = '';
            res.on('data', function(chunk) {
                jsonStr += chunk;
            });
            res.on('end', function() {
                var data = JSON.parse(jsonStr);
                if (data 
                    && data.Response 
                    && data.Response.toString().toLowerCase() == 'true') {
                        var imdb = formatIMDBData(data);
                        client.hset(getMovieKey(hash), 'imdb', JSON.stringify(imdb), function(err, reply) {
                            if (err) {
                            } else {
                                // set douban data 
                                setDoubanData(client, hash, imdb['id'], null); 
                                // download poster image
                                var poster = imdb['Poster'];
                                if (poster && typeof poster == 'string' 
                                    && poster.indexOf('http://') == 0) {
                                    exports.downloadPoster(hash, 
                                        poster, 
                                        path.join(image_save_root, hash + path.extname(poster)), callback);
                                }
                            }
                        });
                    }
            });
        }
    }); 
}

function addAttr(result, attr, newValue) {
    if (attr in result) {
        result[attr] = result[attr] + ' / ' + newValue;
    } else {
        result[attr] = newValue;
    }
}

function formatDoubanData(data) {
    var result = {};
    result['Plot'] = (data['summary'] ? data['summary']['$t'] : '');
    var attrs = data['db:attribute'];
    if (attrs && attrs instanceof Array && attrs.length > 0) {
        for (var i = 0; i < attrs.length; i++) {
            var name = attrs[i]['@name'];
            var t = attrs[i]['$t'];
            var lang = attrs[i]['@lang'];
            if (name == 'movie_duration') {
                result['Runtime'] = t;
            } else if (name == 'writer') {
                addAttr(result, 'Writer', t);
            } else if (name == 'director') {
                addAttr(result, 'Director', t);
            } else if (name == 'pubdate') {
                result['Released'] = t;
            } else if (name == 'aka' && lang == 'zh_CN') {
                result['Title'] = t;
            } else if (name == 'movie_type') {
                addAttr(result, 'Genre', t);
            } else if (name == 'cast') {
                addAttr(result, 'Actors', t);
            }
        }
    }
    var rating = data['gd:rating'];
    if (rating && typeof rating === 'object') {
        result['Votes'] = rating['@numRaters'] + '';
        result['Rating'] = rating['@average'];
    }
    result['id'] = path.basename(data['id']['$t']);
    result['url'] = 'http://movie.douban.com/subject/' + result['id'] + '/';
    return result;
}

function setDoubanData(client, hash, imdbID, callback) {
    var options = {
        host: 'api.douban.com',
        path: '/movie/subject/imdb/' + imdbID + '?' + qs.stringify({
            apikey: '0841eac1af6041aa18c5c0450e20c4fe',
            alt: 'json'
        })
    };
    http.get(options, function(res) {
        if (res.statusCode == 200) {
            res.setEncoding('utf8');
            var jsonStr = '';
            res.on('data', function(chunk) {
                jsonStr += chunk;
            });
            res.on('end', function() {
                var data = JSON.parse(jsonStr);
                if (data && typeof data === 'object') {
                    var douban = formatDoubanData(data);
                    if (callback && callback instanceof Function) {
                        client.hset(getMovieKey(hash), 'douban', JSON.stringify(douban), callback);
                    } else {
                        client.hset(getMovieKey(hash), 'douban', JSON.stringify(douban));
                    }
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

exports.loadMovie = function(hash, callback) {
    var client = this.client;
    var key = getMovieKey(hash);
    client.hgetall(key, function(err, movie) {
        if (callback) {
            var reply = null;
            if (err) {
                reply = {'success': false, 'error': err, 'movie': {}};
            } else {
                reply = {'success': true, 'movie': to_frontend_movie(movie)};
            }
            callback(reply);
        }
    });
}

exports.listMovies = function(type, callback) {
    var client = this.client;
    var key = getMoviesKey(type);
    client.smembers(key, function(err, hashes) {
        if (err) {
        } else {
            if (hashes && util.isArray(hashes) && hashes.length > 0) {
                var multi = client.multi();
                for (var i = 0; i < hashes.length; i++) {
                    multi.hgetall(getMovieKey(hashes[i]));
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
        var key = getMovieKey(m.hash);
        var client = this.client;
        client.hmset(key, m, function(err, reply) {
            if (err == null && reply.toString().toUpperCase() == 'OK') {
                client.multi([
                    ['sadd', getMoviesKey(ALL_MV), m.hash],
                    ['sadd', getMoviesKey(AVAIL_MV), m.hash],
                    ['srem', getMoviesKey(REMOVED_MV), m.hash]
                ]).exec(function(err, replies) {});
            }
        });
    }
}

exports.removeMovie = function(filepath) {
    var m = createMovieObj(filepath, false);
    if (m) {
        var key = getMovieKey(m.hash);
        var client = this.client;
        client.hmset(key, m, function(err, reply) {
            if (err == null && reply.toString().toUpperCase() == 'OK') {
                client.multi([
                    ['sadd', getMoviesKey(REMOVED_MV), m.hash],
                    ['srem', getMoviesKey(AVAIL_MV), m.hash]
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
    var hashes = [];
    for (var i = 0; i < files.length; i++) {
        var filepath = files[i];
        var m = createMovieObj(filepath, true);
        var key = getMovieKey(m.hash);
        hashes[hashes.length] = m.hash;
        multi.hmset(key, m);
    }
    multi.exec(function(err, replies) {
        if (err) {
        } else {
            var multi2 = client.multi();
            multi2.del(getMoviesKey(AVAIL_MV));
            if (hashes.length > 0) {
                multi2.sadd(getMoviesKey(ALL_MV), hashes);
                multi2.sadd(getMoviesKey(AVAIL_MV), hashes);
            }
            multi2.sdiffstore(getMoviesKey(REMOVED_MV), getMoviesKey(ALL_MV), getMoviesKey(AVAIL_MV));
            multi2.exec(function(err, replies) {
                if (err) {
                } else {
                    client.smembers(REMOVED_MV, function(err, reply) {
                        if (err) {
                        } else if (reply && util.isArray(reply) && reply.length > 0) {
                            var multi3 = client.multi();
                            for (var i = 0; i < reply.length; i++) {
                                multi3.hset(getMovieKey(reply[i]), 'available', 'false');
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
    if (m['douban']) {
        m['douban'] = JSON.parse(m['douban']);
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
