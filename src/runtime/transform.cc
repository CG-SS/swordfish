#include "swordfish/runtime/transform.hh"
#include "swordfish/blobl/interp.hh"

#include <memory>

namespace sf {

transform_fn interpreted_transform(blobl::mapping m) {
    // The interpreter holds a reference to the mapping, so the two must share a
    // lifetime; a shared_ptr to both keeps the returned callable copyable.
    struct holder {
        blobl::mapping mapping;
        blobl::interp  interp;
        explicit holder(blobl::mapping m) : mapping(std::move(m)), interp(mapping) {}
    };
    auto h = std::make_shared<holder>(std::move(m));
    return [h](exec_ctx& ctx) { return h->interp.run_ctx(ctx); };
}

} // namespace sf
