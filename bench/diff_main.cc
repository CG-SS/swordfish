// Runs one generated mapping over one JSON document and prints the result,
// so its stdout can be diffed against `sfslice run`.
#include <swordfish/runtime.hh>
#include <swordfish/message.hh>
#include <iostream>
namespace sf::gen { sf::value blobl_0(sf::exec_ctx&); }
int main(int argc, char** argv) {
    const std::string doc = argc > 1 ? argv[1] : "{}";
    try {
        sf::exec_ctx ctx;
        sf::value input = sf::parse_json(doc);
        ctx.this_v = &input;
        // Rendered through set_mapped/as_bytes, exactly as `sfslice run` does,
        // NOT with to_json(). A string root becomes the message content
        // verbatim, so to_json() quoted what the other side printed bare and
        // every string-rooted mapping was reported as a divergence -- 35 of
        // them, none real. A comparison harness that renders differently from
        // the thing it compares against measures its own formatting.
        sf::message out(doc);
        if (out.set_mapped(sf::gen::blobl_0(ctx)))
            std::cout << out.as_bytes() << "\n";
        else
            // set_mapped() is false for a `nothing` or `deleted` root, and
            // `sfslice run` prints the ORIGINAL document in that case. Omitting
            // this branch printed nothing instead and reported every
            // nothing-rooted mapping as a divergence.
            std::cout << doc << "\n";
    } catch (const sf::eval_error& e) {
        std::cerr << "mapping failed: " << e.what() << "\n";
        return 1;
    }
}
