// Synchronous responses: where a pipeline's result goes when the input can
// answer whoever produced the message.
//
// The store travels on the MESSAGE rather than on the transaction, because a
// pipeline splits, filters and recombines batches freely and the store has to
// survive all of that. Benthos puts it in the same place -- the message part's
// context, reached through internal/transaction/result_store.go -- and for the
// same reason.
#pragma once

#include "swordfish/message.hh"

#include <vector>

namespace sf {

// Written by the `sync_response` output, read by the input that created it.
// Nothing else touches it, and an input that cannot answer synchronously simply
// never attaches one -- which is what makes `sync_response` a documented no-op
// against a `file` or `kafka` source rather than an error.
class response_store {
public:
    void add(batch b) { _batches.push_back(std::move(b)); }
    const std::vector<batch>& batches() const noexcept { return _batches; }
    bool                      empty()    const noexcept { return _batches.empty(); }

private:
    std::vector<batch> _batches;
};

} // namespace sf
