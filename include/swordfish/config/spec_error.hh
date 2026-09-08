// The error a config problem is reported as.
//
// It carries the source position, so `lint` can point at the offending line: a
// diagnostic that cannot name a line is close to useless, and matching
// `benthos lint`'s output is a project goal.
//
// It lives HERE rather than in runtime/pipeline_spec.hh, where it started,
// because that header reaches Seastar through transaction.hh and a config error
// has to be throwable from the config layer -- which is built without Seastar on
// its include path. Templates are what found this: they are parsed and expanded
// entirely in the config layer.
#pragma once

#include "swordfish/config/yaml.hh"

#include <stdexcept>
#include <string>

namespace sf {

class spec_error : public std::runtime_error {
public:
    spec_error(cfg::position w, std::string msg)
        : std::runtime_error(std::move(msg)), where(w) {}
    cfg::position where;
};

} // namespace sf
