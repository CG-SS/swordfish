// The `socket` input and output and the `socket_server` input.
//
// Field names and defaults are the reference's, from
// connect-main/docs/modules/components/pages/{inputs,outputs}/socket*.adoc.
//
// `network` offers tcp and unix only. UDP is deliberately absent from the
// options list rather than accepted and approximated: a datagram source has no
// stream to frame, so the `scanner` that governs every other network here would
// mean something different under it, and a config that linted clean would then
// split messages by a rule the author did not choose. `tls` is likewise not a
// field, so a config asking for it fails to lint rather than connecting in the
// clear.
#pragma once

#include "swordfish/config/spec.hh"
#include "swordfish/runtime/component.hh"
#include "swordfish/runtime/scanner.hh"

#include <string>

namespace sf::sock {

struct input_config {
    std::string  network = "tcp";
    std::string  address;
    bool operator==(const input_config&) const = default;
};

struct server_input_config {
    std::string  network = "tcp";
    std::string  address;
    bool operator==(const server_input_config&) const = default;
};

struct output_config {
    std::string network = "tcp";
    std::string address;
    bool operator==(const output_config&) const = default;
};

// The scanner is a separate argument for the same reason the rate limit is:
// `scanner:` names a COMPONENT whose key is its kind, which the spec_of field
// model cannot express, so it is lifted out of the body during parsing.
input_ptr  make_socket_input(const input_config& c, const scanner_spec& sc);
input_ptr  make_socket_server_input(const server_input_config& c, const scanner_spec& sc);
output_ptr make_socket_output(const output_config& c);

// Registration is EXPLICIT, called from register_builtin_io(); see the note on
// sf::http::register_components().
void register_components();

} // namespace sf::sock

namespace sf::cfg {

// Shared by all three: the networks and framings that are actually implemented.
inline constexpr std::string_view socket_networks[] = {"tcp", "unix"};

template <> struct spec_of<sf::sock::input_config> {
    static constexpr std::string_view cpp_type = "sf::sock::input_config";
    static constexpr auto value = object(
        field("network", &sf::sock::input_config::network)
            .describe("The network type to connect over.").options(socket_networks),
        field("address", &sf::sock::input_config::address)
            .describe("The address to connect to.").require()
    );
};

template <> struct spec_of<sf::sock::server_input_config> {
    static constexpr std::string_view cpp_type = "sf::sock::server_input_config";
    static constexpr auto value = object(
        field("network", &sf::sock::server_input_config::network)
            .describe("The network type to listen on.").options(socket_networks),
        field("address", &sf::sock::server_input_config::address)
            .describe("The address to listen on.").require()
    );
};

template <> struct spec_of<sf::sock::output_config> {
    static constexpr std::string_view cpp_type = "sf::sock::output_config";
    static constexpr auto value = object(
        field("network", &sf::sock::output_config::network)
            .describe("The network type to connect over.").options(socket_networks),
        field("address", &sf::sock::output_config::address)
            .describe("The address to connect to.").require()
    );
};

} // namespace sf::cfg
