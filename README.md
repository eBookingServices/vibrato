# vibrato
Fast Librato.com client for vibe.d with local aggregation.


The library spawns a helper thread to perform aggregations and to send the metric data asynchronously.

The goal was to keep impact on the main thread to a minimum. Contention is kept to a minimum by using a double-buffer and performing as much work on the helper thread as possible.

There is a callback that can be used to call user code from the helper thread before performing aggregation and sending.


Example usage:
```d
import vibrato;

enum MyMetrics {
	cpu_user_time = "cpu.user.time",
	cpu_usage = "cpu.usage",
	request_count = "request.count",
	request_time = "request.time",
}

auto settings = Settings("example@librato.com", "75AFDB82", "frontend.0", "frontend.0.");

init(settings);

// bind metric names
registerCounter(cpu_user_time);
registerCounter(request_count);
registerCounter(request_time);

registerGauge(cpu_usage);

// start the sending and aggregation thread
start();
...

counter(cpu_user_time, getCPUUserTime()); // set absolute value
gauge(cpu_usage, getCPUUsage());

...
increment(request_count); // increment by 1
auto requestTimer = StopWatch(AutoStart.yes);
...
timed(request_time, requestTimer.peek);
```

# note
Gauges can take some flags:
```d
enum GaugeFlags : ushort {
	TimeSeconds			= 1 << 0, // by default time is in msecs
	NoAggregate			= 1 << 1, // do not aggregate locally - send every sample and it's timestamp untouched
	AggregateOnlyAvg	= 1 << 2, // compute and send only the average
	AggregateNoStdDev	= 1 << 3, // compute and send everything (count, min, max, sum, ...), except sum_squares
	Default = 0,
}
```