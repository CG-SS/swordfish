#include "swordfish/runtime/transaction.hh"

namespace sf {

ack_fn ack_group::add_child() {
    ++_pending;
    auto self = shared_from_this();
    return [self](std::exception_ptr e) mutable { return self->child_done(e); };
}

seastar::future<> ack_group::child_done(std::exception_ptr e) {
    if (e && !_first_error) _first_error = e;      // first nack wins
    if (--_pending == 0 && _sealed && !_fired) {
        _fired = true;
        return _parent(_first_error);
    }
    return seastar::make_ready_future<>();
}

seastar::future<> ack_group::seal() {
    _sealed = true;
    if (_pending == 0 && !_fired) {
        // Every message was filtered. Benthos treats that as successful
        // delivery, not as loss, so the source still gets an ack.
        _fired = true;
        return _parent(_first_error);
    }
    return seastar::make_ready_future<>();
}

ack_fn noop_ack() {
    return [](std::exception_ptr) { return seastar::make_ready_future<>(); };
}

} // namespace sf
