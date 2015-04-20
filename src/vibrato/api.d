module vibrato.api;


import core.sync.mutex;
import core.thread;
import core.time;

import std.algorithm;
import std.array;
import std.base64;
import std.datetime;
import std.range;
import std.socket;
import std.string;

import vibe.data.json;
import vibe.inet.url;
import vibe.http.client;
import vibe.stream.operations;

import vibrato.json;


class VibratoException : Throwable {
    this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
        super(msg, file, line);
    }
}


alias SendCallback = void delegate();


struct Settings {
	string user;				// librato.com user email
	string token;				// librato.com api token
	string source;				// this instance's source name - defaults to machine's hostname
	string prefix;				// metric name prefix
	size_t intervalSecs = 10;	// send/aggregate thread sleep cycle
	SendCallback sendCallback;	// this callback will be called from the sender thread right after waking up, and before aggregation
	HTTPClientSettings httpSettings;	// http client settings
	string host;				// librato.com's host
	ushort port;				// librato.com's port
	string path;				// librato.com's api path
}


private size_t hashOf(const(char)[] x) {
	size_t hash = 5381;
	foreach(i; 0..x.length)
		hash = (hash * 33) ^ cast(size_t)(std.ascii.toLower(x.ptr[i]));
	return hash;
}


enum GaugeFlags : ushort {
	TimeSeconds			= 1 << 0, // by default time is in msecs
	NoAggregate			= 1 << 1, // do not aggregate locally - send every sample and it's timestamp untouched
	AggregateOnlyAvg	= 1 << 2, // compute and send only the average
	AggregateNoStdDev	= 1 << 3, // compute and send everything (count, min, max, sum, ...), except sum_squares
	Default = 0,
}


struct Annotation {
	struct Link {
		string label;
		string rel;
		string href;
	}

	string stream;
	string title;
	string description;
	string source;
	Link[] links;
	SysTime start;
	SysTime end;
}


void init(Settings settings) {
	settings_ = settings;

	if (settings_.host.empty)
		settings_.host = defaultHostName;
	if (settings_.path.empty)
		settings_.path = defaultPath;
	if (!settings_.port)
		settings_.port = defaultPort;
	if (!settings_.source)
		settings_.source = Socket.hostName;
	if (settings_.httpSettings is null)
		settings_.httpSettings = new HTTPClientSettings;

	auth_ = "Basic " ~ cast(string)Base64.encode(cast(ubyte[])(settings_.user ~ ":" ~ settings_.token));

	metricsURL_ = settings_.path ~ "/metrics";
	annotationsURL_ = "https://" ~ settings_.host ~ "/" ~ settings_.path ~ "/annotations/";

	if (!settings_.prefix.empty) {
		foreach(ref counter; hashCounters_) {
			if (!counter.name.empty)
				counter.prefixed = settings_.prefix.empty ? counter.name : (settings_.prefix ~ counter.name);
		}
		foreach(ref gauge; hashGauges_) {
			if (!gauge.name.empty)
				gauge.prefixed = settings_.prefix.empty ? gauge.name : (settings_.prefix ~ gauge.name);
		}
	}

	thread_ = new Thread(&sender);
	mutex_ = new Mutex();
}


void start() {
	assert(!running_);

	running_ = true;
	thread_.start();
}


void shutdown() {
	running_ = false;
	if (thread_ !is null) {
		thread_.join();
		thread_.destroy();
		thread_ = null;
	}

	if (mutex_ !is null) {
		mutex_.destroy();
		mutex_ = null;
	}

	hashCounters_.clear;
	hashGauges_.clear;

	dataGauges_ = null;
	dataCounters_ = null;
}


private string unescapeError(string x) {
	if (!x.empty && (x[0] == '\"'))
		x = x.dropOne.dropBack(1).replace("\\\"", "\"").replace("\\n", "\n").replace("\\t", "\t");
	return x;
}


private string libratoError(scope HTTPClientResponse res) {
	string error = res.bodyReader.readAllUTF8;

	if (auto ContentType = "Content-Type" in res.headers) {
		if ((*ContentType).indexOf("/json") != -1) {
			auto json = error.parseJsonString;
			if (auto perrorsJson = "errors" in json) {
				foreach(string type, errorJson; *perrorsJson) {
					if (errorJson.type == Json.Type.array) {
						error = null;
						foreach(errorLine; errorJson)
							error ~= (error.length ? "\n" : null) ~ errorLine.get!string.unescapeError;
					} else if (errorJson.type == Json.Type.string) {
						error = errorJson.get!string.unescapeError;
					}
				}
			}
		}
	}

	throw new VibratoException(format("Librato.com error: %s", error));
}


void annotation(Annotation event) {
	assert(!event.stream.empty, "must specify an annotation stream name");
	assert(!event.title.empty, "must specify a title for this event");

	auto json = jsonWriter(2048);
	{
		json.beginObject();
		scope(exit) json.endObject();

		json.field("name").value(event.stream);
		json.field("title").value(event.title);
		if (!event.description.empty)
			json.field("title").value(event.title);
		if (!event.description.empty)
			json.field("description").value(event.description);
		if (!event.source.empty)
			json.field("source").value(event.source);
		if (event.start.stdTime != 0)
			json.field("start_time").value(event.start.toUTC.toUnixTime);
		if (event.end.stdTime != 0)
			json.field("end_time").value(event.end.toUTC.toUnixTime);
		if (!event.links.empty) {
			json.field("links");

			json.beginArray();
			scope(exit) json.endArray();

			foreach(l; event.links) {
				json.beginObject();
				scope(exit) json.endObject();

				assert(!l.rel.empty);
				assert(!l.href.empty);
				json.field("rel").value(l.rel);
				json.field("href").value(l.href);
				if (!l.label.empty)
					json.field("label").value(l.label);
			}
		}
	}

	auto url = annotationsURL_ ~ event.stream;
	requestHTTP(url, (scope HTTPClientRequest req) {
		req.method = HTTPMethod.POST;
		req.requestURL = url;
		req.headers["Content-Type"] = "application/json";
		req.headers["Authorization"] = auth_;
		req.bodyWriter.write(json.json);
	}, (scope HTTPClientResponse res) {
		if ((res.statusCode != HTTPStatus.OK) && (res.statusCode != HTTPStatus.Created))
			libratoError(res);
		res.dropBody();
	});
}


void registerCounter(string name) {
	assert(!running_, "cannot call register() once started");
	assert(name !in hashGauges_, "'" ~ name ~ "' is already registered as a gauge");

	auto info = hashCounters_.insert(name);
	assert(info);

	if (info.name.empty) {
		info.name = name;
		info.prefixed = settings_.prefix.empty ? name : (settings_.prefix ~ name);
		info.slot = cast(ushort)hashCounters_.length;

		dataCounters_[0] ~= 0.0;
		dataCounters_[1] ~= 0.0;
	} else {
		assert(info.name == name);
		info.prefixed = settings_.prefix.empty ? name : (settings_.prefix ~ name);
	}
	assert(dataCounters_[0].length == dataCounters_[1].length);
}


void registerGauge(string name, GaugeFlags flags = GaugeFlags.Default) {
	assert(!running_, "cannot call register() once started");
	assert(name !in hashCounters_, "'" ~ name ~ "' is already registered as a counter");

	auto info = hashGauges_.insert(name);
	assert(info);

	if (info.name.empty) {
		info.name = name;
		info.prefixed = settings_.prefix.empty ? name : (settings_.prefix ~ name);
		info.slot = cast(ushort)hashGauges_.length;
		info.flags = flags;

		++dataGauges_[0].length;
		++dataGauges_[1].length;
	} else {
		assert(info.name == name);
		info.prefixed = settings_.prefix.empty ? name : (settings_.prefix ~ name);
		info.flags = flags;
	}
	assert(dataGauges_[0].length == dataGauges_[1].length);
}


// ok to access settings_ and ungarded as there is no interface to change settings
private void sender() {
	HTTPClient http = connectHTTP(settings_.host, settings_.port, true, settings_.httpSettings);

	auto json = jsonWriter(8192);

	while (running_) {
		Thread.sleep(settings_.intervalSecs.seconds);

		if (settings_.sendCallback)
			settings_.sendCallback();

		size_t buffer = buffer_;
		synchronized(mutex_) {
			buffer_ = (buffer_ + 1) & 1;
		}

		auto now = Clock.currTime(UTC()).toUnixTime;

		{
			json.clear();
			json.beginObject();
			scope(exit) json.endObject();

			json.field("counters");
			{
				json.beginArray();
				scope(exit) json.endArray();

				foreach (ref counter; hashCounters_) {
					auto pvalue = &dataCounters_.ptr[buffer].ptr[counter.slot];
					json.beginObject();
					scope(exit) json.endObject();

					json.field("name").value(counter.prefixed);
					json.field("value").value(*pvalue);
				}
			}

			json.field("gauges");
			{
				json.beginArray();
				scope(exit) json.endArray();

				foreach(ref gauge; hashGauges_) {
					auto pgauge = &dataGauges_.ptr[buffer].ptr[gauge.slot];
					auto flags = gauge.flags;

					// note: these aggregations could well be SSE'd
					if (!pgauge.values.empty) {
						if ((flags & GaugeFlags.NoAggregate) == 0) {
							json.beginObject();
							scope(exit) json.endObject();

							json.field("name").value(gauge.prefixed);

							auto vsum = 0.0;
							auto vsumSq = 0.0;
							auto vmin = double.max;
							auto vmax = -double.max;
							auto count = pgauge.values.length;

							if (flags & GaugeFlags.AggregateOnlyAvg) {
								foreach(value; pgauge.values)
									vsum += value;
								json.field("value").value(vsum / cast(double)count);
							} else if (flags & GaugeFlags.AggregateNoStdDev) {
								foreach(value; pgauge.values) {
									vsum += value;
									vmin = min(vmin, value);
									vmax = max(vmax, value);
								}
								json.field("count").value(count);
								json.field("sum").value(vsum);
								json.field("min").value(vmin);
								json.field("max").value(vmax);
							} else {
								foreach(value; pgauge.values) {
									vsum += value;
									vsumSq += value * value;
									vmin = min(vmin, value);
									vmax = max(vmax, value);
								}
								json.field("count").value(count);
								json.field("sum").value(vsum);
								json.field("min").value(vmin);
								json.field("max").value(vmax);
								json.field("sum_squares").value(vsumSq);
							}
						} else {
							foreach(i, value; pgauge.values) {
								json.beginObject();
								scope(exit) json.endObject();

								json.field("name").value(gauge.prefixed);
								json.field("value").value(value);
								json.field("measure_time").value(pgauge.times[i].stdTimeToUnixTime);
							}
						}
						pgauge.values.length = 0;
						pgauge.times.length = 0;
					}
				}
			}

			json.field("source").value(settings_.source);
			json.field("measure_time").value(now);
		}

		http.request((scope HTTPClientRequest req) {
			req.method = HTTPMethod.POST;
			req.requestURL = metricsURL_;
			req.headers["Content-Type"] = "application/json";
			req.headers["Authorization"] = auth_;
			req.bodyWriter.write(json.json);
		}, (scope HTTPClientResponse res) {
			if (res.statusCode != HTTPStatus.OK)
				libratoError(res);
			res.dropBody();
		});
	}

	http.disconnect;
	http.destroy;
	running_ = false;
}


void increment(string name, double count = 1.0) {
	assert(running_, "cannot increment before start() has been called");

	if (auto counter = hashCounters_.find(name)) {
		synchronized(mutex_) {
			dataCounters_.ptr[buffer_].ptr[counter.slot] += count;
		}
	} else {
		assert(false, "unknown counter metric '" ~ name ~ "'");
	}
}


void counter(string name, double count) {
	assert(running_, "cannot set counter before start() has been called");

	if (auto counter = hashCounters_.find(name)) {
		synchronized(mutex_) {
			dataCounters_.ptr[buffer_].ptr[counter.slot] = count;
		}
	} else {
		assert(false, "unknown counter metric '" ~ name ~ "'");
	}
}


private void gaugei(alias Filter)(string name, double value) {
	assert(running_, "cannot add gauge sample before start() has been called");

	if (auto gauge = hashGauges_.find(name)) {
		auto flags = gauge.flags;
		synchronized(mutex_) {
			auto pgauge = &dataGauges_.ptr[buffer_].ptr[gauge.slot];
			pgauge.values ~= Filter(flags, value);
			if (flags & GaugeFlags.NoAggregate)
				pgauge.times ~= Clock.currTime(UTC()).stdTime; // converted to unixtime by sender thread
		}
	} else {
		assert(false, "unknown gauge metric '" ~ name ~ "'");
	}
}


void gauge(string name, double value) {
	gaugei!((f, x) => x)(name, value);
}

void timed(string name, double milliseconds) {
	gaugei!((flags, x) => (flags & GaugeFlags.TimeSeconds) ? (x * 0.001) : x)(name, milliseconds);
}

void timed(string name, TickDuration duration) {
	gaugei!((flags, x) => (flags & GaugeFlags.TimeSeconds) ? (x * 0.001) : x)(name, cast(double)duration.msecs);
}

void timed(string name, Duration duration) {
	gaugei!((flags, x) => (flags & GaugeFlags.TimeSeconds) ? (x * 0.001) : x)(name, cast(double)duration.total!"msecs");
}

void timed(string name, SysTime start, SysTime end) {
	gaugei!((flags, x) => (flags & GaugeFlags.TimeSeconds) ? (x * 0.001) : x)(name, cast(double)(end - start).total!"msecs");
}


shared static ~this() {
	shutdown();
}


private enum defaultHostName = "metrics-api.librato.com";
private enum defaultPath = "/v1";
private enum defaultPort = 443;


private struct GaugeValue {
	double[] values;
	long[] times;
}


private struct MetricInfo {
	ushort slot;
	ushort flags;
	string name;
	string prefixed;
}


private __gshared static {
	Settings settings_;

	string auth_;
	string metricsURL_;
	string annotationsURL_;

	MetricMap!MetricInfo hashCounters_;
	MetricMap!MetricInfo hashGauges_;

	double[][2] dataCounters_;
	GaugeValue[][2] dataGauges_;

	Mutex mutex_;
	Thread thread_;

	shared bool running_;
	shared size_t buffer_;
}


private struct MetricMap(T) {
	T* find(string name) {
		size_t probeCount = metrics_.length;
		size_t mask = (probeCount - 1);
		size_t base = hashOf(name) & mask;
		size_t probe = 0;

		while (probe != probeCount) {
			size_t index = base + ((probe >> 1) + ((probe * probe) >> 1)) + (probe & 1);

			auto metric = &metrics_[index & mask];
			if (!metric.name.empty) {
				if (metric.name == name)
					return metric;
				return null;
			}
			++probe;
		}
		return null;
	}

	T* insert(string name) {
		if (count_ >= ((3 * metrics_.length) >> 2)) // ensure at most 75% load
			grow();

		size_t probeCount = metrics_.length;
		size_t mask = (probeCount - 1);
		size_t base = hashOf(name) & mask;
		size_t probe = 0;

		while (probe != probeCount) {
			size_t index = base + ((probe >> 1) + ((probe * probe) >> 1)) + (probe & 1);

			auto metric = &metrics_[index & mask];
			if (metric.name.empty) {
				++count_;
				return metric;
			} else if (metric.name == name) {
				return metric;
			}

			++probe;
		}
		assert(0);
	}

	void grow() {
		T[] metrics;

		const size = max(32, metrics_.length) << 1;
		const mask = size - 1;
		metrics.length = size;

		foreach(ref metric; metrics_) {
			if (!metric.name.empty)
				metrics[hashOf(metric.name) & mask] = metric;
		}

		swap(metrics_, metrics);
	}

	void clear() {
		count_ = 0;
		metrics_ = null;
	}

	@property auto length() const {
		return count_;
	}

	@property auto capacity() const {
		return metrics_.length;
	}

	T* opIn_r(in string name) {
        return find(name);
    }

	int opApply(scope int delegate(ref T) dg) {
        foreach(ref metric; metrics_) {
            if (!metric.name.empty) {
                if (auto res = dg(metric))
                    return res;
            }
        }
        return 0;
    }

	private T[] metrics_;
	private size_t count_;
}
