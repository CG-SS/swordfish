// Transactions and acknowledgement — the unit of flow.
//
// Mirrors benthos-main/internal/message/transaction.go: a batch plus a means of
// telling the source whether delivery succeeded. Acks travel backwards, and any
// component that splits a batch must aggregate the children's acks so the parent
// is resolved exactly once.
#pragma once

#include "swordfish/message.hh"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/util/noncopyable_function.hh>

#include <exception>

namespace sf {

// A nullptr exception_ptr means success. A non-null one is a nack: the source
// should redeliver, which is what makes the pipeline at-least-once.
using ack_fn = seastar::noncopyable_function<seastar::future<>(std::exception_ptr)>;

struct transaction {
    batch                  payload;
    ack_fn                 ack;
    seastar::abort_source* abort = nullptr;   // fail-fast for blocked deliveries

    transaction() = default;
    transaction(batch p, ack_fn a, seastar::abort_source* as = nullptr) noexcept
        : payload(std::move(p)), ack(std::move(a)), abort(as) {}
    transaction(transaction&&) noexcept = default;
    transaction& operator=(transaction&&) noexcept = default;
};

// Splits (for_each, split, output brokers, switch) turn one transaction into
// several. The parent must be acked only once every child has completed, and
// nacked if any child failed.
//
// Rules, matching Benthos:
//   - the first error wins; later ones are dropped
//   - a filtered message (zero results) counts as SUCCESS, not as a loss
//   - the parent ack fires exactly once, when the last child completes
class ack_group : public seastar::enable_lw_shared_from_this<ack_group> {
public:
    static seastar::lw_shared_ptr<ack_group> make(ack_fn parent) {
        return seastar::make_lw_shared<ack_group>(std::move(parent));
    }
    explicit ack_group(ack_fn parent) : _parent(std::move(parent)) {}

    // Must be called for every child BEFORE any child completes, otherwise the
    // count can reach zero early and the parent acks while work is outstanding.
    ack_fn add_child();

    // Call once all children have been created. Handles the zero-child case,
    // where the parent must still be acked.
    seastar::future<> seal();

private:
    seastar::future<> child_done(std::exception_ptr e);

    ack_fn             _parent;
    size_t             _pending = 0;
    bool               _sealed  = false;
    bool               _fired   = false;
    std::exception_ptr _first_error;
};

// Helper: an ack that does nothing, for sources with no delivery guarantee.
ack_fn noop_ack();

} // namespace sf
