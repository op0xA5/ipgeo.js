
(function(global) {

	var _ArrayBuffer = window.ArrayBuffer,
		_Uint8Array = window.Uint8Array,
		_DataView = window.DataView,
		_TextDecoder = window.TextDecoder;

	global.isIpGeoSupported = function() {
		return !!_ArrayBuffer && !!_Uint8Array && !! _DataView && !!_TextDecoder;
	}

	var _persistentStorage = navigator.persistentStorage || navigator.webkitPersistentStorage,
		_requestFileSystem = window.requestFileSystem || window.webkitRequestFileSystem,
		_Blob = window.Blob;
	global.isIpGeoLoclaSupported = function() {
		return global.isIpipSupported() && !!_persistentStorage && !!_requestFileSystem && !!_Blob;
	}

	var xhrGet = function(url, onLoaded, onFailed) {
		var xhr = new XMLHttpRequest();
		xhr.responseType = "arraybuffer";
		xhr.open("GET", url);
		xhr.onreadystatechange = function() {
			if (xhr.readyState == 4) {
				if (xhr.status == 200) {
					var dat = xhr.response;
					if (dat == null || !dat instanceof _ArrayBuffer || !dat.byteLength) {
						onFailed("read http failed: data not received");
						return
					}
					return onLoaded(dat);
				} 
				onFailed("read http failed: " + xhr.statusText);
			}
		}
		xhr.send();
	}

	var requestedBytes = 128 * 1024 * 1024;
	var reqFileSystem = function (onInitFs, onFailed) {
		_persistentStorage.requestQuota(requestedBytes, function(grantedBytes) {
			_requestFileSystem(window.PERSISTENT, grantedBytes, onInitFs, onFailed);
		}, onFailed);
	};
	var localGet = function(filename, onLoaded, onFailed) {
		reqFileSystem(function(fs) {
			fs.root.getFile(filename, { create: false, exclusive: true }, function(fileEntry) {
				fileEntry.file(function(file) {
					var r = new FileReader();
					r.onload = function() {
						onLoaded(r.result);
					}
					r.onerror = function() {
						onFailed('error reading file', r.error);
					}
					r.readAsArrayBuffer(file);					
				}, function (err) {
					onFailed('error access file', err);
				});
			}, function (err) {
				onFailed('error access file', err);
			});
		}, function (err) {
			onFailed('error requestFileSystem', err);
		});
	};
	global.ipGeo = function(config, onLoaded, onFailed) {
		var filename = config && config.filename,
			url = config && config.url,
			backup = config && config.backup,
			type = config && config.type;

		if (!(type == 'ipip' || type == 'cz88')) {
			panic('type should be ipip or cz88');
		}
		if (backup && !filename) filename = url.split('/').pop();

		var onLocalLoaded = function(dat) {
			if (type == 'ipip') {
				onLoaded(function(ip) {
					return ipipFinder(dat, ip);
				});
			} else if (type == 'cz88') {
				onLoaded(function(ip) {
					return cz88Finder(dat, ip);
				});
			}
		};
		var onXhrLoad = function(dat) {
			if (filename) {
				reqFileSystem(function(fs) {
					fs.root.getFile(filename, { create: true, exclusive: false }, function(fileEntry) {
						fileEntry.createWriter(function(w) {
							w.onwriteend = function(e) {
								console.log('Write completed. Local ipGeo Database updated');
							};

							w.onerror = function(e) {
								console.log('Write failed: ' + e.toString());
							};

							w.write(new _Blob([ dat ], { type: 'application/octet-stream' }));
						}, function (err) {
							console.log('createWriter Failed: ' + err);
						});
					}, function (err) {
						console.log('getFile Failed: ' + err);
					});
				}, function (err) {
					console.log('Request FileSystem Failed: ' + err);
				});
			}

			onLocalLoaded(dat);
		};
		if (url) {
			if (backup) {
				// backup mode: try read local file first, if failed, try fetch from http
				if (!filename) panic('local filename not set');
				localGet(filename, onLocalLoaded, function() {
					xhrGet(url, onXhrLoad, onFailed);
				});
				return;
			}
			// normal mode: fetch from http, then save to local file if filename is set
			xhrGet(url, onXhrLoad, function(err, msg) {
				if (filename) {
					localGet(localfilename, onLocalLoaded, onFailed);
				} else {
					onFailed(err, msg);
				}
			});
		} else if (filename) {
			// load from local file only
			localGet(filename, onLocalLoaded, onFailed);
		} else {
			panic('local filename or url not set');
		}
	}

	var binarySearch = function(start, end, fn) {
		while(start < end) {
			var mid = start + ((end - start) >> 1);
			if (!fn(mid)) {
				start = mid + 1;
			} else {
				end = mid;
			}
		}
		return start;
	}

	var utf8Decoder = new TextDecoder('UTF-8');
	var ipStrToUint = function(ip) {
		var m = ip.match(/^\s*(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\s*$/);
		if (!m) return false;
		return parseInt(m[1]) * 256 * 256 * 256 + parseInt(m[2]) * 256 * 256 + parseInt(m[3]) * 256 + parseInt(m[4]);
	}
	var ipipFinder = function(buf, ip) {
		var ipUint = ipStrToUint(ip);
		if (ipUint === false) return false;

		var offset = new _DataView(buf).getUint32(0);
		if (offset == 0 || offset > buf.byteLength) return false;

		var dvIndexMap = new _DataView(buf, 4); 

		// ipip database divided indexes into 256 pieces, splited by first byte of ip address
		var ip1 = ((ipUint >> 24) + 256) % 256;
		var start = dvIndexMap.getUint32(ip1 * 4, true),
			end = ip1 == 0xFF ? (offset - 4 - 1024 - 1024) / 8 - 1 : dvIndexMap.getUint32(ip1 * 4 + 4, true);

		// do binary search
		var dvIndex = new _DataView(buf, 4 + 1024);
		var r = binarySearch(start, end, function(n) {
			return dvIndex.getUint32(n * 8) >= ipUint;
		});
		if (r < start) return [ '未知' ];

		var textOffset = offset - 1024 + (dvIndex.getUint32(r * 8 + 4, true) & 0xFFFFFF),
			textCount = dvIndex.getUint8(r * 8 + 7);

		var txt = utf8Decoder.decode(buf.slice(textOffset, textOffset + textCount));
		return txt.split('\t').filter(function(str) { return str != "" });
	}

	var gbkDecoder = new TextDecoder('GBK');
	var cz88Finder = function(buf, ip) {
		var ipUint = ipStrToUint(ip);
		if (ipUint === false) return false;

		var dv = new _DataView(buf);
		var indexOffset = dv.getUint32(0, true),
			indexCount = (dv.getUint32(4, true) - indexOffset) / 7 - 1;

		var dvIndex = new _DataView(buf, indexOffset);
		var r = binarySearch(0, indexCount, function(n) {
			return dvIndex.getUint32(n * 7, true) >= ipUint;
		});
		if (r < 0) return [ '未知' ];
		var offset = dvIndex.getUint32(r * 7 + 4, true) & 0xFFFFFF;
		if (offset == 0) return [ '未知' ];

		var result = [];
		var _readString = function(addr) {
			if (addr == 0) {
				result.push('未知');
				return 0;
			}
			// string record ended with "\0"
			var end = addr;
			while (dv.getUint8(end)) end++;
			result.push(gbkDecoder.decode(buf.slice(addr, end)).replace('CZ88.NET', ''));
			return end;
		}

		var _readStringRedirect = function(addr) {
			var redirect = dv.getUint8(addr);
			if (redirect == 1) {
				return _readStringRedirect(dv.getUint32(addr + 1, true) & 0xFFFFFF);
			} else if (redirect == 2) {
				_readString(dv.getUint32(addr + 1, true) & 0xFFFFFF);
				return addr + 4;
			}
			return _readString(addr);
		}
		offset = _readStringRedirect(offset + 4);
		_readStringRedirect(offset);
		return result;
	}

})(window);

