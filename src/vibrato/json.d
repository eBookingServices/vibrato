module vibrato.json;


import std.format;
import std.range;
import std.traits;


void escapeJSONString(Appender, T)(ref Appender app, T value) if (isSomeString!T) {
	app.put('"');
	auto start = value.ptr;
	auto end = value.ptr + value.length;
	auto ptr = start;
	while (ptr != end) {
		switch(*ptr) {
			case '"':
				app.put(start[0..ptr-start]);
				start = ptr + 1;
				app.put('\\');
				app.put('"');
				break;
			case '\\':
				app.put(start[0..ptr-start+1]);
				start = ptr + 1;
				app.put('\\');
				break;
			case '\n':
				app.put(start[0..ptr-start]);
				start = ptr + 1;
				app.put('\\');
				app.put('n');
				break;
			case '\r':
				app.put(start[0..ptr-start]);
				start = ptr + 1;
				app.put('\\');
				app.put('r');
				break;
			case '\t':
				app.put(start[0..ptr-start]);
				start = ptr + 1;
				app.put('\\');
				app.put('t');
				break;
			case '\b':
				app.put(start[0..ptr-start]);
				start = ptr + 1;
				app.put('\\');
				app.put('b');
				break;
			case '\f':
				app.put(start[0..ptr-start]);
				start = ptr + 1;
				app.put('\\');
				app.put('f');
				break;
			default:
				break;
		}

		++ptr;
	}
	app.put(start[0..ptr-start]);
	app.put('"');
}


private struct JSONFragment {
	private this(size_t alloc) {
		json_.reserve(alloc);
		states_.reserve(8);
		states_ ~= StateInfo(States.Ready, 0);
	}

	auto ref clear() {
		json_.clear;
		states_.length = 0;
		states_ ~= StateInfo(States.Ready, 0);

		return this;
	}

	auto ref beginArray() {
		assert((states_.back.state != States.Finished) && (states_.back.state != States.InObject));

		if (states_.back.state != States.InField) {
			if (states_.back.count > 0)
				json_.put(',');
			++states_.back.count;
		}

		states_ ~= StateInfo(States.InArray);
		json_.put('[');

		return this;
	}

	auto ref endArray() {
		assert(states_.back.state == States.InArray);

		--states_.length;
		json_.put(']');

		if (states_.back.state == States.Ready) {
			states_.back.state = States.Finished;
		} else if (states_.back.state == States.InField) {
			states_.back.state = States.InObject;
		}

		return this;
	}

	auto ref beginObject() {
		assert((states_.back.state != States.Finished) && (states_.back.state != States.InObject));

		if (states_.back.state != States.InField) {
			if (states_.back.count > 0)
				json_.put(',');
			++states_.back.count;
		}

		states_ ~= StateInfo(States.InObject);
		json_.put('{');

		return this;
	}

	auto ref endObject() {
		assert(states_.back.state == States.InObject);

		--states_.length;
		json_.put('}');

		if (states_.back.state == States.Ready) {
			states_.back.state = States.Finished;
		} else if (states_.back.state == States.InField) {
			states_.back.state = States.InObject;
		}

		return this;
	}

	auto ref value(T)(in T x) {
		assert((states_.back.state != States.Finished) && (states_.back.state != States.InObject));

		if (states_.back.state != States.InField) {
			if (states_.back.count > 0)
				json_.put(',');
			++states_.back.count;
		}

		static if (isSomeString!T) {
			json_.escapeJSONString(x);
		} else static if (isArray!T) {
			beginArray();
			static if (!is(Unqual!(typeof(T.init[0])) == void)) {
				foreach(ref item; x)
					value(item);
			}
			endArray();
		} else static if (isAssociativeArray!T) {
			beginObject();
			foreach(ref key, ref item; x) {
				field(key);
				value(item);
			}
			endObject();
		} else static if (is(T == bool)) {
			json_.put(x ? "true" : "false");
		} else static if (is(T == struct)) {
			beginObject();
			static if (__traits(allMembers, T).length) {
				foreach(member; __traits(allMembers, T)) {
					field(member);
					value(__traits(getMember, x, member));
				}
			}
			endObject();
		} else {
			json_.formattedWrite("%s", x);
		}

		if (states_.back.state == States.Ready) {
			states_.back.state = States.Finished;
		} else if (states_.back.state == States.InField) {
			states_.back.state = States.InObject;
		}

		return this;
	}

	auto ref field(T)(in T name) {
		assert(states_.back.state == States.InObject);

		if (states_.back.count > 0)
			json_.put(',');
		++states_.back.count;

		states_.back.state = States.InField;

		static if (isSomeString!T) {
			json_.escapeJSONString(name);
		} else {
			json_.formattedWrite("\"%s\"", name);
		}
		json_.put(':');


		return this;
	}

	@property const(char)[] json() {
		assert((states_.length == 1) && (states_.back.state == States.Finished));
		return json_.data;
	}

	enum States : uint {
		Ready = 0,
		InArray,
		InObject,
		InField,
		Finished,
	}

	struct StateInfo {
		States state;
		uint count;
	}

	private Appender!(char[]) json_;
	private StateInfo[] states_;
}


auto ref jsonWriter(size_t alloc = 2048) {
	auto frag = JSONFragment(alloc);
	return frag;
}
