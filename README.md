# vibrato
Fast Librato.com client for vibe.d with local aggregation.


The library spawns a helper thread to perform aggregations and to send the metric data asynchronously.

The goal was to keep impact on the main thread to a minimum. Contention is kept to a minimum by using a double-buffer and performing as much work on the helper thread as possible.

There is a callback that can be used to call user code from the helper thread before performing aggregation and sending.


Example usage:
```d
import vibrato;

enum MyMetrics {
	cpu_user_time,
	cpu_usage,
	page_views,
	request_time,
}

auto settings = Settings("example@librato.com", "75AFDB82", "frontend.0", "frontend.0.");

init(settings);

// bind metric names
metric(cpu_user_time, cpu_user_time.stringof, MetricType.Gauge);
metric(cpu_usage, cpu_usage.stringof, MetricType.Gauge);
metric(page_views, page_views.stringof, MetricType.Counter);
metric(request_time, request_time.stringof, MetricType.Gauge);

// start the sending and aggregation thread
start();

...

increment(page_views);

auto requestTimer = StopWatch(AutoStart.yes);
...
timed(request_time, requestTimer.peek);

...
gauge(cpu_usage, getCPUUsage());
```

# notes
Have a look at src/vibrato/api.d for a list of all options
