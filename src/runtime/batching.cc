#include "swordfish/runtime/batching.hh"

#include <seastar/util/log.hh>

namespace sf {

static seastar::logger blog("sf.batching");

batch_policy::batch_policy(int64_t count, int64_t byte_size,
                           std::chrono::milliseconds period, transform_fn check)
    : _count(count), _byte_size(byte_size), _period(period), _check(std::move(check)),
      _last_batch(std::chrono::steady_clock::now()) {}

bool batch_policy::add(message m) {
    // Measured BEFORE the append and only when a byte trigger exists:
    // as_bytes() serialises a structured message, which is the expensive part
    // of this whole class.
    if (_byte_size > 0) _size_tally += static_cast<int64_t>(m.as_bytes().size());
    _parts.push_back(std::move(m));

    // Order matters only in that each test is skipped once something has
    // already triggered -- the check in particular, which runs a mapping.
    if (!_triggered && _count > 0 && static_cast<int64_t>(_parts.size()) >= _count)
        _triggered = true;
    if (!_triggered && _byte_size > 0 && _size_tally >= _byte_size)
        _triggered = true;
    if (!_triggered && _check) {
        exec_ctx ctx;
        value parsed;
        message& last = _parts.back();
        bool structured = true;
        try { parsed = last.as_structured(); }
        catch (const eval_error&) { structured = false; }
        ctx.this_v = structured ? &parsed : nullptr;
        ctx.msg = &last;
        ctx.meta = &last.meta();
        ctx.meta_in = &last.meta();
        ctx.all = &_parts;
        ctx.batch_index = static_cast<int64_t>(_parts.size()) - 1;
        ctx.batch_size = static_cast<int64_t>(_parts.size());
        try {
            _triggered = truthy(_check(ctx));
        } catch (const std::exception& e) {
            // Not a trigger, and said out loud: a check that throws every time
            // would otherwise leave the batch to the period trigger with no
            // indication of why.
            blog.error("batching check failed: {}", e.what());
        }
    }

    // The period is reported here as well as by the caller's timer, so a policy
    // driven only by arriving messages still honours it.
    return _triggered || (_period.count() > 0 &&
                          std::chrono::steady_clock::now() - _last_batch > _period);
}

std::chrono::milliseconds batch_policy::until_next() const {
    if (_period.count() <= 0) return std::chrono::milliseconds{0};
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - _last_batch);
    return elapsed >= _period ? std::chrono::milliseconds{0} : _period - elapsed;
}

batch batch_policy::take() {
    batch out = std::move(_parts);
    _parts.clear();
    _size_tally = 0;
    _triggered = false;
    // Restarted on every flush, including one the period did not cause: the
    // period means "no message waits longer than this", not "flush every this".
    _last_batch = std::chrono::steady_clock::now();
    return out;
}

} // namespace sf
