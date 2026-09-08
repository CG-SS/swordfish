// A bounded hand-off point between MANY producers and MANY consumers.
//
// `seastar::queue` is deliberately not this. Its blocking paths keep exactly one
// promise for a waiting producer and one for a waiting consumer:
//
//     _not_full = promise<>();      // in queue::not_full()
//
// so a second waiter on either side OVERWRITES the first's promise, and the
// first fails with `broken_promise`. That is correct for the single-fiber
// pipeline queues it was written for, and wrong the moment a server is on one
// end: every connection is a producer, and every request handler is a consumer.
//
// The symptom is not a crash. It is a request that fails for no visible reason
// while an unrelated one succeeds, which for the `http_server` output looked
// like "the third GET returns nothing" and for the `http_server` input would
// have been an occasional spurious 503 under concurrent posts. A gate that
// issues requests one at a time cannot see any of it.
#pragma once

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <chrono>
#include <deque>
#include <exception>
#include <utility>

namespace sf {

// A condition_variable can hold any number of waiters -- they live on an
// intrusive list and broadcast() wakes them all -- so it is the primitive this
// is built on. Every wait re-checks its own predicate on waking, which is what
// makes several waiters racing for one item correct rather than merely likely.
//
// There is no lost-wakeup window: the predicate check and condition_variable::
// wait() run with no suspension point between them, and a shard is cooperative,
// so nothing can slip in and signal in between.
template <class T>
class rendezvous {
public:
    explicit rendezvous(size_t capacity) : _capacity(capacity ? capacity : 1) {}

    // Waits while the buffer is full. Throws whatever close() was given once
    // the rendezvous is closed.
    seastar::future<> push(T v) {
        while (!_ex && _q.size() >= _capacity) co_await _cv.wait();
        if (_ex) std::rethrow_exception(_ex);
        _q.push_back(std::move(v));
        _cv.broadcast();
    }

    // As push(), but gives up at `deadline` with
    // seastar::condition_variable_timed_out. Nothing is enqueued unless there
    // was actually room, so a timeout cannot smuggle an item in after the caller
    // has given up on it -- the mirror of the guarantee pop(deadline) makes.
    template <class Clock, class Duration>
    seastar::future<> push(T v, std::chrono::time_point<Clock, Duration> deadline) {
        while (!_ex && _q.size() >= _capacity) co_await _cv.wait(deadline);
        if (_ex) std::rethrow_exception(_ex);
        _q.push_back(std::move(v));
        _cv.broadcast();
    }

    // Waits while the buffer is empty.
    seastar::future<T> pop() {
        while (!_ex && _q.empty()) co_await _cv.wait();
        if (_ex) std::rethrow_exception(_ex);
        co_return take();
    }

    // As pop(), but gives up at `deadline` with
    // seastar::condition_variable_timed_out. Nothing is removed until an item
    // has actually been seen, so a timeout cannot swallow one -- which is the
    // failure mode of wrapping a queue pop in seastar::with_timeout, where the
    // abandoned pop still takes an item and then drops it.
    template <class Clock, class Duration>
    seastar::future<T> pop(std::chrono::time_point<Clock, Duration> deadline) {
        while (!_ex && _q.empty()) co_await _cv.wait(deadline);
        if (_ex) std::rethrow_exception(_ex);
        co_return take();
    }

    // As pop(), but also gives up when `as` is aborted. `as` may be null, which
    // is an ordinary pop.
    //
    // For a consumer whose REASON TO EXIST can disappear: an http_server
    // streaming handler whose client has disconnected would otherwise park here
    // for ever and -- worse -- keep competing for batches it cannot deliver,
    // taking them from the handlers that still have readers.
    //
    // It wakes periodically rather than breaking the condition variable, because
    // the cv is shared with every other consumer and breaking it would take them
    // all down with this one. The granularity only bounds how long a dead
    // consumer keeps competing, so it is coarse on purpose.
    seastar::future<T> pop(seastar::abort_source* as) {
        if (!as) co_return co_await pop();
        while (!_ex && _q.empty()) {
            if (as->abort_requested()) throw seastar::abort_requested_exception();
            try {
                co_await _cv.wait(std::chrono::steady_clock::now() +
                                  std::chrono::milliseconds(100));
            } catch (const seastar::condition_variable_timed_out&) {
                // Round again, to re-check the abort.
            }
        }
        if (_ex) std::rethrow_exception(_ex);
        if (as->abort_requested()) throw seastar::abort_requested_exception();
        co_return take();
    }

    // Wakes every waiter with `e` and refuses everything afterwards. Whatever
    // was still buffered is handed back rather than discarded: each entry
    // usually has a producer waiting on a promise inside it, and dropping those
    // silently would leave those fibers blocked for ever.
    std::deque<T> close(std::exception_ptr e) {
        if (!_ex) _ex = e;
        auto left = std::move(_q);
        _q.clear();
        _cv.broadcast();
        return left;
    }

    bool   closed() const noexcept { return static_cast<bool>(_ex); }
    size_t size()   const noexcept { return _q.size(); }

private:
    T take() {
        T v = std::move(_q.front());
        _q.pop_front();
        // Broadcast on REMOVAL as well as on insertion: a producer blocked on a
        // full buffer is waiting for exactly this.
        _cv.broadcast();
        return v;
    }

    std::deque<T>               _q;
    size_t                      _capacity;
    std::exception_ptr          _ex;
    seastar::condition_variable _cv;
};

} // namespace sf
