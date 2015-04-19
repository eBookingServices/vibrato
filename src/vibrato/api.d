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


enum MetricType {
	None = 0,
	Counter,
	Gauge,
}

// max 10 bits (MetricInfo.FlagBits)
enum MetricFlags {
	TimeSeconds			= 1 << 0,	// by default time is in msecs
	NoAggregate			= 1 << 1,	// do not aggregate locally - send every sample directly
	AggregateOnlyAvg	= 1 << 2,	// compute only the average
	AggregateNoStdDev	= 1 << 3,	// compute everything (count, min, max, sum, ...), except sum_squares
	Default = 0,
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
		foreach(ref metric; metrics_) {
			if (metric.type != MetricType.None)
				metric.name = settings_.prefix ~ metric.name;
		}
	}

	thread_ = new Thread(&sender);
	mutex_ = new Mutex();
}


void start() {
	assert(!running_);

	dataCounters_[0].length = counters_;
	dataCounters_[1].length = counters_;
	dataGauges_[0].length = gauges_;
	dataGauges_[1].length = gauges_;

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

	dataGauges_ = null;
	dataCounters_ = null;
	metrics_ = null;
	counters_ = 0;
	gauges_ = 0;
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


void metric(size_t index, string name, MetricType type, MetricFlags flags = MetricFlags.Default) {
	assert(!running_, "cannot call metric() once started");
	assert(type != MetricType.None);

	if (index >= metrics_.length) {
		auto length = max(64, metrics_.length);
		while (index >= length) {
			length = min(length >> 2, length + 64);
		}
		metrics_.length = length;
	}

	metrics_[index] = MetricInfo(((thread_ !is null) ? (settings_.prefix ~ name) : name), type, flags, (type == MetricType.Counter) ? counters_++ : gauges_++);
}


// ok to access settings_ and http_ ungarded as there is no interface to change settings
private void sender() {
	HTTPClient http = connectHTTP(settings_.host, settings_.port, true, settings_.httpSettings);

	auto json = jsonWriter(8192);

	while (running_) {
		Thread.sleep(settings_.intervalSecs.seconds);

		if (settings_.sendCallback)
			settings_.sendCallback();

		size_t index = index_;
		synchronized(mutex_) {
			index_ = (index_ + 1) & 1;
		}

		size_t measurementCount = 0;
		{
			json.clear();
			json.beginObject();
			scope(exit) json.endObject();

			json.field("counters");
			{
				json.beginArray();
				scope(exit) json.endArray();

				foreach (ref metric; metrics_) {
					if (metric.type == MetricType.Counter) {
						auto slot = metric.slot;
						auto pvalue = &dataCounters_.ptr[index].ptr[slot];
						if (*pvalue) {
							json.beginObject();
							scope(exit) json.endObject();

							json.field("name").value(metric.name);
							json.field("value").value(*pvalue);
							++measurementCount;
							*pvalue = 0;
						}
					}
				}
			}

			json.field("gauges");
			{
				json.beginArray();
				scope(exit) json.endArray();

				foreach(ref metric; metrics_) {
					if (metric.type == MetricType.Gauge) {
						auto slot = metric.slot;
						auto flags = metric.flags;
						auto pgauge = &dataGauges_.ptr[index].ptr[metric.slot];

						// note: these aggregations could well be SSE'd
						if (!pgauge.values.empty) {
							if ((flags & MetricFlags.NoAggregate) == 0) {
								json.beginObject();
								scope(exit) json.endObject();

								json.field("name").value(metric.name);

								auto vsum = 0.0;
								auto vsumSq = 0.0;
								auto vmin = double.max;
								auto vmax = -double.max;
								auto count = pgauge.values.length;

								if (flags & MetricFlags.AggregateOnlyAvg) {
									foreach(value; pgauge.values)
										vsum += value;
									json.field("value").value(vsum / cast(double)count);
								} else if (flags & MetricFlags.AggregateNoStdDev) {
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
								++measurementCount;
							} else {
								foreach(i, value; pgauge.values) {
									json.beginObject();
									scope(exit) json.endObject();

									json.field("name").value(metric.name);
									json.field("measure_time").value(pgauge.times[i].stdTimeToUnixTime);
									json.field("value").value(value);
								}
							}
							pgauge.values.length = 0;
							pgauge.times.length = 0;
						}
					}
				}
			}

			if (measurementCount) {
				json.field("source").value(settings_.source);
				json.field("measure_time").value(Clock.currTime(UTC()).toUnixTime);
			}
		}

		if (measurementCount) {
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
	}

	http.disconnect;
	http.destroy;
	running_ = false;
}


void increment(size_t metric, size_t count = 1) {
	assert(running_, "cannot increment before start() has been called");
	assert(metric < metrics_.length, "metric index out of range");
	assert(metrics_[metric].type == MetricType.Counter, "index is not assigned to a counter metric");
	
	auto slot = metrics_.ptr[metric].slot;
	assert(slot < dataCounters_.length);

	synchronized(mutex_) {
		dataCounters_.ptr[index_].ptr[slot] += count;
	}
}


private void gaugei(alias Filter)(size_t metric, double value) {
	assert(running_, "cannot add gauge value before start() has been called");
	assert(metric < metrics_.length, "metric index out of range");
	assert(metrics_[metric].type == MetricType.Gauge, "index is not assigned to a counter metric");

	auto flags = metrics_.ptr[metric].flags;
	auto slot = metrics_.ptr[metric].slot;
	assert(slot < dataGauges_.length);

	synchronized(mutex_) {
		auto pgauge = &dataGauges_.ptr[index_].ptr[slot];
		pgauge.values ~= Filter(flags, value);
		if (flags & MetricFlags.NoAggregate) {
			pgauge.times ~= Clock.currTime(UTC()).stdTime; // converted to unixtime by sender thread
		}
	}
}


void gauge(size_t metric, double value) {
	gaugei!((f, x) => x)(metric, value);
}

void timed(size_t metric, double milliseconds) {
	gaugei!((flags, x) => (flags & MetricFlags.TimeSeconds) ? (x * 0.001) : x)(metric, milliseconds);
}

void timed(size_t metric, TickDuration duration) {
	gaugei!((flags, x) => (flags & MetricFlags.TimeSeconds) ? (x * 0.001) : x)(metric, cast(double)duration.msecs);
}

void timed(size_t metric, SysTime start, SysTime end) {
	gaugei!((flags, x) => (flags & MetricFlags.TimeSeconds) ? (x * 0.001) : x)(metric, cast(double)(end - start).total!"msecs");
}


shared static ~this() {
	shutdown();
}


private enum defaultHostName = "metrics-api.librato.com";
private enum defaultPath = "/v1";
private enum defaultPort = 443;


private struct MetricInfo {
	this(string name, MetricType type, MetricFlags flags, size_t slot) {
		assert(slot < (1 << SlotBits));
		flags_ = slot | (flags << SlotBits) | (type << (SlotBits + FlagBits));
		name_ = name;
	}

	private enum {
		SlotBits	= 20,
		FlagBits	= 10,
		TypeBits	= 2,
	}

	@property MetricType type() const {
		return cast(MetricType)(flags_ >> (SlotBits + FlagBits));
	}

	@property MetricFlags flags() const {
		return cast(MetricFlags)((flags_ >> SlotBits) & ((1 << FlagBits) - 1));
	}

	@property size_t slot() const {
		return flags_ & ((1 << SlotBits) - 1);
	}

	@property string name() const {
		return name_;
	}

	@property void name(string name) {
		name_ = name;
	}

	private size_t flags_;
	private string name_;
}


private shared struct GaugeValue {
	double[] values;
	long[] times;
}


private __gshared static {
	Settings settings_;

	string auth_;
	string metricsURL_;
	string annotationsURL_;

	size_t counters_;
	size_t gauges_;
	MetricInfo[] metrics_;

	ulong[][2] dataCounters_;
	GaugeValue[][2] dataGauges_;

	Mutex mutex_;
	Thread thread_;

	shared bool running_;
	shared size_t index_;
}