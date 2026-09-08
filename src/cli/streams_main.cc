// `swordfish streams` — many pipelines in one process, with a REST API.
//
//   swordfish streams ./configs/*.yaml
//   swordfish streams -o ./root.yaml ./configs
//
// A stream's ID is its filename without the extension, which is what the
// reference does and what every URL in its API is keyed by. The `-o` config is
// the ROOT config: it carries only service-wide fields (`http`, and the engine
// block), never an input or an output, because those belong to the streams.
//
// Each stream is held exactly as `swordfish run` holds its one stream -- a
// `sharded<stream_service>`, an independent input/pipeline/output per shard --
// so a config behaves identically under `run` and under `streams`. That is the
// property that makes this worth having rather than a second runtime with its
// own bugs.
//
// EVERYTHING that touches the stream table happens on shard 0. An HTTP request
// is served on whichever shard accepted the connection, and `sharded<>` may
// only be started and stopped from the shard that created it; a DELETE arriving
// on shard 3 would otherwise tear down a container it does not own. The
// handlers hop with submit_to(0, ...) and the table is never locked, because
// only one shard ever sees it.

#include "swordfish/cli/commands.hh"
#include "swordfish/config/template.hh"
#include "swordfish/config/yaml.hh"
#include "swordfish/kafka/components.hh"
#include "swordfish/runtime/build_stream.hh"
#include "swordfish/runtime/components.hh"
#include "swordfish/runtime/observe.hh"
#include "swordfish/runtime/pipeline_spec.hh"
#include "swordfish/runtime/stream_service.hh"

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/signal.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/timer.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/util/log.hh>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <vector>

using namespace sf;
using namespace sf::cfg;

static seastar::logger slog("sf.streams");

namespace {

// ---- one running stream --------------------------------------------------

struct running_stream {
    std::string                              id;
    std::string                              source;   // the file it came from
    // The document as it was given, env already resolved. Kept because the
    // reference's GET /streams/{id} echoes the config back and a caller may be
    // reading it; swordfish compiles a config into factories and has nothing to
    // re-serialise from the spec, so the way to answer is to remember what came
    // in. Held per stream rather than re-read from disk: a stream created
    // through POST never had a file.
    cfg::ynode                               doc;
    stream_spec                              spec;
    std::unique_ptr<seastar::sharded<stream_service>> shards;
    std::chrono::steady_clock::time_point    started_at{};
    bool                                     finished = false;

    double uptime_seconds() const {
        const auto now = std::chrono::steady_clock::now();
        return std::chrono::duration<double>(now - started_at).count();
    }
};

// Go's time.Duration.String(), which is what the reference puts in
// `uptime_str`. Not a cosmetic choice: a dashboard that parses the field sees
// "1.5s" and "2m30.5s", never "1.500000s".
std::string go_duration(double seconds) {
    if (seconds < 1e-6) return "0s";
    auto trim = [](std::string s) {
        if (s.find('.') == std::string::npos) return s;
        while (!s.empty() && s.back() == '0') s.pop_back();
        if (!s.empty() && s.back() == '.') s.pop_back();
        return s;
    };
    char buf[64];
    if (seconds < 1e-3) {
        std::snprintf(buf, sizeof buf, "%.6f", seconds * 1e6);
        return trim(buf) + "µs";
    }
    if (seconds < 1.0) {
        std::snprintf(buf, sizeof buf, "%.6f", seconds * 1e3);
        return trim(buf) + "ms";
    }
    std::string out;
    auto whole = static_cast<long long>(seconds);
    const double frac = seconds - static_cast<double>(whole);
    const long long h = whole / 3600;
    const long long m = (whole % 3600) / 60;
    const double    s = static_cast<double>(whole % 60) + frac;
    if (h) out += std::to_string(h) + "h";
    if (h || m) out += std::to_string(m) + "m";
    std::snprintf(buf, sizeof buf, "%.9f", s);
    out += trim(buf) + "s";
    return out;
}

std::string json_escape(std::string_view s) {
    std::string out;
    for (const char c : s) {
        switch (c) {
        case '"':  out += "\\\""; break;
        case '\\': out += "\\\\"; break;
        case '\n': out += "\\n";  break;
        case '\r': out += "\\r";  break;
        case '\t': out += "\\t";  break;
        default:
            if (static_cast<unsigned char>(c) < 0x20) {
                char b[8];
                std::snprintf(b, sizeof b, "\\u%04x", c);
                out += b;
            } else {
                out += c;
            }
        }
    }
    return out;
}

// ---- the manager, which lives only on shard 0 ----------------------------

class stream_manager {
public:
    // Started, and added to the table only once it is running: a stream that
    // fails to start must not appear in GET /streams as though it were fine.
    seastar::future<> add(std::string id, std::string source, cfg::ynode doc,
                          stream_spec spec) {
        if (_streams.count(id))
            throw std::runtime_error("stream '" + id + "' already exists");
        auto rs = seastar::make_lw_shared<running_stream>();
        rs->id = id;
        rs->source = std::move(source);
        rs->doc = std::move(doc);
        rs->spec = std::move(spec);
        rs->shards = std::make_unique<seastar::sharded<stream_service>>();
        co_await rs->shards->start(rs->spec);
        // The unwind is OUTSIDE the catch: C++20 forbids co_await in a handler,
        // and sharded<> asserts if it is destroyed without stop(), so a failed
        // start has to be undone rather than dropped.
        std::exception_ptr start_failed;
        try {
            co_await rs->shards->invoke_on_all(&stream_service::start);
        } catch (...) {
            start_failed = std::current_exception();
        }
        if (start_failed) {
            co_await rs->shards->stop();
            std::rethrow_exception(start_failed);
        }
        rs->started_at = std::chrono::steady_clock::now();
        slog.info("stream '{}' started from {}", id, rs->source);
        _streams.emplace(std::move(id), std::move(rs));
    }

    seastar::future<bool> remove(const std::string& id) {
        const auto it = _streams.find(id);
        if (it == _streams.end()) co_return false;
        // Copied out and erased BEFORE anything suspends, so a second DELETE
        // for the same id finds nothing and cannot tear it down twice.
        auto rs = it->second;
        _streams.erase(it);
        // Drained, not killed: a stream removed through the API has in-flight
        // messages that are already the source's responsibility, and dropping
        // them would break the exactly-once-ack rule the whole runtime is
        // built on.
        co_await rs->shards->invoke_on_all([](stream_service& s) {
            s.drain();
            return seastar::make_ready_future<>();
        });
        co_await rs->shards->invoke_on_all(&stream_service::wait);
        co_await rs->shards->stop();
        slog.info("stream '{}' removed", id);
        co_return true;
    }

    bool has(const std::string& id) const { return _streams.count(id) != 0; }

    // Aggregated across every stream and every shard.
    seastar::future<observed> gather() {
        observed total;
        for (auto& [id, rs] : snapshot()) {
            const observed o = co_await sf::gather_from(*rs->shards);
            total.messages_in += o.messages_in;   total.messages_out += o.messages_out;
            total.batches_in  += o.batches_in;    total.batches_out  += o.batches_out;
            total.acks        += o.acks;          total.nacks        += o.nacks;
            total.filtered    += o.filtered;      total.proc_errors  += o.proc_errors;
            total.not_ready_in  += o.not_ready_in;
            total.not_ready_out += o.not_ready_out;
        }
        // With NO streams the answer is ready, which is the reference's rule
        // too: "If there are no active streams 200 is returned."
        total.ready = total.not_ready_in == 0 && total.not_ready_out == 0;
        co_return total;
    }

    seastar::future<observed> gather_one(const std::string& id) {
        const auto it = _streams.find(id);
        if (it == _streams.end()) co_return observed{};
        const auto rs = it->second;          // survives the suspension below
        co_return co_await sf::gather_from(*rs->shards);
    }

    // `active` is the reference's IsRunning(): a stream whose source has been
    // exhausted is listed, with its uptime, but is no longer active.
    seastar::future<std::string> list_json() {
        std::string out = "{";
        bool first = true;
        for (auto& [id, rs] : snapshot()) {
            const observed o = co_await sf::gather_from(*rs->shards);
            if (!first) out += ',';
            first = false;
            out += '"' + json_escape(id) + "\":" + info_json(*rs, o);
        }
        out += "}\n";
        co_return out;
    }

    seastar::future<std::string> one_json(const std::string& id) {
        const auto it = _streams.find(id);
        if (it == _streams.end()) co_return std::string();
        const auto rs = it->second;          // survives the suspension below
        const observed o = co_await sf::gather_from(*rs->shards);
        // With the config, which GET /streams/{id} carries and GET /streams
        // does not -- the same split the reference makes, so a listing of a
        // hundred streams does not become a hundred configs.
        std::string out = info_json(*rs, o);
        out.pop_back();                      // the closing brace
        out += ",\"config\":" + cfg::node_to_value(rs->doc).to_json() + "}\n";
        co_return out;
    }

    std::vector<std::string> ids() const {
        std::vector<std::string> v;
        v.reserve(_streams.size());
        for (const auto& [id, rs] : _streams) v.push_back(id);
        return v;
    }

    // Every stream drained and stopped, for process shutdown.
    seastar::future<> stop_all() {
        const auto all = snapshot();
        for (auto& [id, rs] : all) {
            co_await rs->shards->invoke_on_all([](stream_service& s) {
                s.drain();
                return seastar::make_ready_future<>();
            });
        }
        for (auto& [id, rs] : all) {
            try {
                co_await rs->shards->invoke_on_all(&stream_service::wait);
            } catch (const std::exception& e) {
                slog.warn("stream '{}' did not drain cleanly: {}", id, e.what());
            }
            co_await rs->shards->stop();
        }
        _streams.clear();
    }

    // Every stream run to natural completion. Only reached with the API off,
    // where the process has nothing to serve and should behave like `run`.
    seastar::future<> wait_all() {
        for (auto& [id, rs] : snapshot())
            co_await rs->shards->invoke_on_all(&stream_service::wait);
    }

    seastar::future<> force_all() {
        for (auto& [id, rs] : snapshot())
            co_await rs->shards->invoke_on_all(&stream_service::force_stop);
    }

private:
    static std::string info_json(const running_stream& rs, const observed& o) {
        // Shape copied from the reference: active, uptime, uptime_str. A
        // dashboard written against redpanda-connect's streams API reads this
        // unchanged, which is the only reason to use its field names.
        const bool active = o.not_ready_in == 0 && o.not_ready_out == 0;
        const double up = rs.uptime_seconds();
        char buf[64];
        std::snprintf(buf, sizeof buf, "%.9g", up);
        return std::string("{\"active\":") + (active ? "true" : "false") +
               ",\"uptime\":" + buf +
               ",\"uptime_str\":\"" + go_duration(up) + "\"}";
    }

    // seastar::lw_shared_ptr, not unique_ptr, and the difference is a
    // use-after-free. Every method below suspends -- gather_from() is a
    // cross-shard map -- and a DELETE arriving on another connection runs on
    // shard 0 while they are suspended, erasing the entry. An iterator or a raw
    // pointer taken before the co_await is dangling when it resumes; a shared
    // pointer copied to a local keeps the stream alive until the reader is
    // done. cppcheck found this one (derefInvalidIteratorRedundantCheck) and it
    // was right: the concurrent DELETE that reaches it is exactly what the API
    // is FOR.
    std::map<std::string, seastar::lw_shared_ptr<running_stream>> _streams;

    // The table as it is right now. Iterating _streams directly across a
    // co_await has the same hazard as holding one iterator, and worse: the loop
    // iterator itself is invalidated.
    std::vector<std::pair<std::string, seastar::lw_shared_ptr<running_stream>>> snapshot() const {
        return {_streams.begin(), _streams.end()};
    }
};

// ---- loading configs from the command line -------------------------------

// The reference keys its API by the filename without its extension, and walks a
// directory argument. A nested file becomes `sub/name`, which is why the id is
// built from the path relative to the directory given rather than the basename.
void collect(const std::string& path, std::vector<std::pair<std::string, std::string>>& out) {
    namespace fs = std::filesystem;
    std::error_code ec;
    if (fs::is_directory(path, ec)) {
        std::vector<fs::path> found;
        for (fs::recursive_directory_iterator it(path, ec), end; it != end; it.increment(ec)) {
            if (ec) break;
            if (!it->is_regular_file()) continue;
            const auto ext = it->path().extension().string();
            if (ext != ".yaml" && ext != ".yml") continue;
            found.push_back(it->path());
        }
        // Sorted, so the same directory always yields the same order and a
        // duplicate-id clash is reported against the same file every run.
        std::sort(found.begin(), found.end());
        for (const auto& f : found) {
            auto rel = fs::relative(f, path, ec).string();
            const auto dot = rel.rfind('.');
            if (dot != std::string::npos) rel.erase(dot);
            out.emplace_back(rel, f.string());
        }
        return;
    }
    auto id = fs::path(path).filename().string();
    const auto dot = id.rfind('.');
    if (dot != std::string::npos) id.erase(dot);
    out.emplace_back(std::move(id), path);
}

// A stream config must not carry the service-wide blocks: those belong to the
// root config, and silently honouring one stream's `http` block would give
// whichever stream happened to load first the whole process's endpoint.
void reject_root_only_fields(const std::string& source, const ynode& doc) {
    for (const char* field : {"http", "logger", "metrics", "tracer"}) {
        if (doc.find(field) != nullptr)
            throw std::runtime_error(
                std::string("a stream config cannot set `") + field +
                "`: it is a service-wide field and belongs in the root config "
                "passed with -o (" + source + ")");
    }
}

// ---- the REST API -------------------------------------------------------
//
// Every handler hops to shard 0 with submit_to, because that is where the
// stream table lives and where `sharded<>` may be started and stopped. The
// request itself is served on whichever shard accepted the connection.

using json_reply = seastar::future<std::unique_ptr<seastar::http::reply>>;

json_reply respond(std::unique_ptr<seastar::http::reply> rep, seastar::http::reply::status_type st,
                   std::string body) {
    rep->set_status(st);
    rep->write_body("json", seastar::sstring(body));
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

json_reply error_reply(std::unique_ptr<seastar::http::reply> rep,
                       seastar::http::reply::status_type st, const std::string& msg) {
    return respond(std::move(rep), st, "{\"error\":\"" + json_escape(msg) + "\"}\n");
}

// Reads a request body. Seastar hands the server side a stream rather than a
// string, and a POST with no body is a real case -- it is how a caller asks for
// a stream with an empty config, which has to be REFUSED rather than crash.
seastar::future<std::string> read_body(seastar::http::request& req) {
    if (req.content_stream == nullptr) co_return std::string();
    std::string out;
    for (;;) {
        auto buf = co_await req.content_stream->read();
        if (buf.empty()) break;
        out.append(buf.get(), buf.size());
    }
    co_return out;
}

// `/streams/<rest>` arrives as one remainder parameter. Splitting it here
// rather than registering two routes keeps `/streams/x` and `/streams/x/stats`
// on one matcher, which is what stops `/streams/x/wibble` being silently served
// as if it were `/streams/x`.
struct stream_path {
    std::string id;
    std::string tail;      // "", "stats", or whatever was actually asked for
    bool        ok = false;
};

stream_path split_path(const seastar::sstring& remainder) {
    stream_path p;
    std::string r(remainder.begin(), remainder.end());
    if (!r.empty() && r.front() == '/') r.erase(0, 1);
    if (r.empty()) return p;
    const auto slash = r.find('/');
    if (slash == std::string::npos) {
        p.id = r;
    } else {
        p.id = r.substr(0, slash);
        p.tail = r.substr(slash + 1);
    }
    p.ok = !p.id.empty();
    return p;
}

std::string stats_json(const observed& o) {
    return std::string("{\"input\":{\"received\":") + std::to_string(o.messages_in) +
           "},\"output\":{\"sent\":" + std::to_string(o.messages_out) +
           "},\"batches\":{\"received\":" + std::to_string(o.batches_in) +
           ",\"sent\":" + std::to_string(o.batches_out) +
           "},\"acks\":" + std::to_string(o.acks) +
           ",\"nacks\":" + std::to_string(o.nacks) +
           ",\"filtered\":" + std::to_string(o.filtered) +
           ",\"processor_errors\":" + std::to_string(o.proc_errors) + "}\n";
}

// Builds a stream from a request body, on shard 0. Every failure here is the
// caller's config being wrong, so it is a 400 with the parser's own message
// rather than a 500.
seastar::future<> add_from_body(stream_manager* m, std::string id, const std::string& body,
                                const stream_spec& root) {
    const ynode doc = sf::cfg::load_config_text(body, "<request body>");
    reject_root_only_fields("<request body>", doc);
    stream_spec spec = build_stream_spec(doc);
    spec.config = root.config;
    co_await m->add(std::move(id), "<API>", doc, std::move(spec));
}

void register_streams_api(seastar::httpd::routes& r, stream_manager* m, stream_spec root) {
    namespace hh = seastar::httpd;
    using seastar::http::reply;

    // GET /streams -- every stream with its status and uptime.
    r.add(hh::operation_type::GET, hh::url("/streams"),
          new hh::function_handler(
              [m](std::unique_ptr<seastar::http::request>,
                  std::unique_ptr<seastar::http::reply> rep) -> json_reply {
                  auto body = co_await seastar::smp::submit_to(0, [m] { return m->list_json(); });
                  co_return co_await respond(std::move(rep), reply::status_type::ok, body);
              },
              "json"));

    // POST /streams -- the reference replaces the whole set. Named rather than
    // silently doing something else: a caller that expects a replace and gets a
    // no-op would find out from its data, not from the response.
    r.add(hh::operation_type::POST, hh::url("/streams"),
          new hh::function_handler(
              [](std::unique_ptr<seastar::http::request>,
                 std::unique_ptr<seastar::http::reply> rep) -> json_reply {
                  co_return co_await error_reply(
                      std::move(rep), reply::status_type::not_implemented,
                      "POST /streams (replace every stream at once) is not implemented "
                      "by swordfish; create them one at a time with POST /streams/{id}");
              },
              "json"));

    const auto one = [m, root](std::unique_ptr<seastar::http::request> req,
                               std::unique_ptr<seastar::http::reply> rep) -> json_reply {
        const stream_path p = split_path(req->get_path_param("path"));
        if (!p.ok)
            co_return co_await error_reply(std::move(rep), reply::status_type::bad_request,
                                           "a stream id is required");
        const auto method = req->_method;

        if (!p.tail.empty() && p.tail != "stats")
            co_return co_await error_reply(
                std::move(rep), reply::status_type::not_found,
                "no such endpoint: /streams/" + p.id + "/" + p.tail);

        if (p.tail == "stats") {
            if (method != "GET")
                co_return co_await error_reply(std::move(rep),
                                               reply::status_type::not_implemented,
                                               method + " on /streams/{id}/stats is not "
                                               "implemented by swordfish");
            const bool exists = co_await seastar::smp::submit_to(
                0, [m, id = p.id] { return m->has(id); });
            if (!exists)
                co_return co_await error_reply(std::move(rep), reply::status_type::not_found,
                                               "no stream '" + p.id + "'");
            const observed o = co_await seastar::smp::submit_to(
                0, [m, id = p.id] { return m->gather_one(id); });
            co_return co_await respond(std::move(rep), reply::status_type::ok, stats_json(o));
        }

        if (method == "GET") {
            auto body = co_await seastar::smp::submit_to(
                0, [m, id = p.id] { return m->one_json(id); });
            if (body.empty())
                co_return co_await error_reply(std::move(rep), reply::status_type::not_found,
                                               "no stream '" + p.id + "'");
            co_return co_await respond(std::move(rep), reply::status_type::ok, body);
        }

        if (method == "DELETE") {
            const bool gone = co_await seastar::smp::submit_to(
                0, [m, id = p.id] { return m->remove(id); });
            if (!gone)
                co_return co_await error_reply(std::move(rep), reply::status_type::not_found,
                                               "no stream '" + p.id + "'");
            co_return co_await respond(std::move(rep), reply::status_type::ok, "{}\n");
        }

        if (method == "POST" || method == "PUT") {
            std::string body = co_await read_body(*req);
            if (body.empty())
                co_return co_await error_reply(std::move(rep), reply::status_type::bad_request,
                                               "the request body must be a stream config");
            if (method == "POST") {
                const bool exists = co_await seastar::smp::submit_to(
                    0, [m, id = p.id] { return m->has(id); });
                if (exists)
                    co_return co_await error_reply(
                        std::move(rep), reply::status_type::bad_request,
                        "stream '" + p.id + "' already exists; use PUT to replace it");
            } else {
                // Replace: the old one is drained first, so a PUT never leaves
                // two streams reading the same source.
                co_await seastar::smp::submit_to(0, [m, id = p.id] { return m->remove(id); });
            }
            // The message is captured rather than answered inside the handler:
            // C++20 forbids co_await in a catch block, and every failure here is
            // the caller's config being wrong, so it is a 400 carrying the
            // parser's own words rather than a 500.
            std::string failure;
            try {
                co_await seastar::smp::submit_to(
                    0, [m, id = p.id, body, root]() mutable {
                        return add_from_body(m, std::move(id), std::move(body), std::move(root));
                    });
            } catch (const std::exception& e) {
                failure = e.what();
            }
            if (!failure.empty())
                co_return co_await error_reply(std::move(rep), reply::status_type::bad_request,
                                               failure);
            co_return co_await respond(std::move(rep), reply::status_type::ok, "{}\n");
        }

        co_return co_await error_reply(std::move(rep), reply::status_type::not_implemented,
                                       method + " on /streams/{id} is not implemented by "
                                       "swordfish; GET, POST, PUT and DELETE are");
    };

    for (const auto op : {hh::operation_type::GET, hh::operation_type::POST,
                          hh::operation_type::PUT, hh::operation_type::DELETE,
                          hh::operation_type::PATCH})
        r.add(op, hh::url("/streams").remainder("path"),
              new hh::function_handler(one, "json"));
}

} // namespace

int sf::cli::streams_main(int argc, char** argv) {
    sf::kafka::register_components();

    seastar::app_template::seastar_options ao;
    ao.name = "swordfish streams";
    seastar::app_template app(std::move(ao));
    app.add_options()
        ("observability,o", boost::program_options::value<std::string>(),
         "root config: service-wide fields only (http, and the engine block)")
        ("templates,t", boost::program_options::value<std::vector<std::string>>()->composing(),
         "import config templates; accepts a file, a directory or a glob")
        ("no-api", "do not serve the streams REST API");
    app.add_positional_options({{"configs",
                                 boost::program_options::value<std::vector<std::string>>()
                                     ->multitoken(),
                                 "stream config files or directories", -1}});

    return app.run(argc, argv, [&app, argv]() -> seastar::future<int> {
        auto& args = app.configuration();

        // The root config carries only service-wide fields. An input or output
        // there is a mistake worth naming: it looks like it would run, and it
        // would not.
        // Before any config is read, including a stream posted to the API
        // later: the registry is process-wide and a template usage is expanded
        // where its kind is looked up.
        if (args.count("templates")) {
            try {
                for (const auto& t : args["templates"].as<std::vector<std::string>>())
                    sf::tmpl::load_templates(t);
            } catch (const std::exception& e) {
                std::cerr << "templates: " << e.what() << "\n";
                co_return 1;
            }
        }

        stream_spec root;
        if (args.count("observability")) {
            const auto path = args["observability"].as<std::string>();
            try {
                const ynode doc = load_config(path);
                for (const char* field : {"input", "output", "pipeline"}) {
                    if (doc.find(field) != nullptr)
                        throw std::runtime_error(
                            std::string("the root config cannot set `") + field +
                            "`: it carries service-wide fields only, and the "
                            "streams themselves come from the positional arguments");
                }
                // Through the SAME parser a stream config goes through, with
                // a throwaway input and output substituted in -- so the root
                // config's `http` block gets every check (its TLS refusals, its
                // address validation) instead of a second, thinner reading of
                // the same fields.
                root = build_stream_spec(sf::with_trivial_io(doc));
            } catch (const std::exception& e) {
                std::cerr << path << ": " << e.what() << "\n";
                co_return 1;
            }
        }

        std::vector<std::pair<std::string, std::string>> found;   // id, path
        if (args.count("configs")) {
            for (const auto& p : args["configs"].as<std::vector<std::string>>()) {
                try {
                    collect(p, found);
                } catch (const std::exception& e) {
                    std::cerr << p << ": " << e.what() << "\n";
                    co_return 1;
                }
            }
        }

        auto mgr = std::make_unique<stream_manager>();
        std::exception_ptr err;
        std::unique_ptr<sf::observe_server> obs;
        auto live      = seastar::make_lw_shared<bool>(true);
        auto force     = seastar::make_lw_shared<seastar::timer<>>();
        auto stopped   = seastar::make_lw_shared<seastar::promise<>>();
        auto signalled = seastar::make_lw_shared<bool>(false);

        try {
            for (const auto& [id, path] : found) {
                const ynode doc = load_config(path);
                reject_root_only_fields(path, doc);
                stream_spec spec = build_stream_spec(doc);
                // The engine-level config comes from the ROOT, so every stream
                // shares one shutdown budget and one error-handling policy --
                // as they share one process.
                spec.config = root.config;
                co_await mgr->add(id, path, doc, std::move(spec));
            }
            if (found.empty())
                slog.info("no stream configs given; serving the API with no streams");

            // The API and the observability endpoints share one address,
            // because the reference serves them from one `http` block and
            // /ready has to answer for every stream at once.
            const bool api = args.count("no-api") == 0;
            auto* m = mgr.get();
            http_spec http = root.http;
            if (!api && !http.enabled) {
                // Nothing to serve and nothing to wait for.
                slog.info("--no-api and no http block: running the streams to completion");
            }
            if (api) http.enabled = true;
            const bool serving = http.enabled;

            obs = std::make_unique<sf::observe_server>(
                http,
                [m]() { return m->gather(); },
                api ? std::function<void(seastar::httpd::routes&)>(
                          [m, root](seastar::httpd::routes& r) { register_streams_api(r, m, root); })
                    : std::function<void(seastar::httpd::routes&)>());
            co_await obs->start();

            const auto budget = root.config.shutdown_timeout;
            auto* mp = mgr.get();
            force->set_callback([mp, live, budget] {
                if (!*live) return;
                slog.warn("shutdown did not complete within {}ms of the signal; forcing",
                          budget.count());
                (void)mp->force_all();
            });
            // Nothing below captures this coroutine's frame by reference.
            // Seastar never unregisters a signal handler, so one delivered
            // after the frame is gone would dereference freed memory -- the
            // same trap run_main.cc records having segfaulted on.
            for (const int sig : {SIGINT, SIGTERM})
                seastar::handle_signal(sig, [mp, live, force, sig, budget, stopped, signalled] {
                    if (!*live || *signalled) return;
                    *signalled = true;
                    slog.info("signal {} received, draining {} stream(s)", sig,
                              mp->ids().size());
                    if (!force->armed()) force->arm(budget);
                    // The drain runs in the background and fulfils the promise
                    // the main coroutine is parked on. set_value() must happen
                    // exactly once, which `signalled` guarantees.
                    (void)mp->stop_all().then_wrapped([stopped](auto f) {
                        f.ignore_ready_future();
                        stopped->set_value();
                    });
                }, true);

            // With the API up the process serves until it is signalled -- that
            // is what streams mode IS, and it is the one place swordfish waits
            // on something other than its data. Without it there is nothing to
            // serve, so it behaves like `run` and exits when the streams are
            // done.
            if (serving) co_await stopped->get_future();
            else         co_await mgr->wait_all();
        } catch (...) {
            err = std::current_exception();
        }

        *live = false;
        force->cancel();
        if (obs) {
            try { co_await obs->stop(); }
            catch (const std::exception& e) { slog.warn("http server stop failed: {}", e.what()); }
        }
        co_await mgr->stop_all();

        if (err) {
            try { std::rethrow_exception(err); }
            catch (const std::exception& e) { std::cerr << "swordfish streams: " << e.what() << "\n"; }
            co_return 1;
        }
        co_return 0;
    });
}
