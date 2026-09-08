// Timestamps: parsing, formatting and calendar arithmetic.
//
// Three format languages have to be supported, because the reference exposes
// all three:
//
//   * Go's reference-time layout ("2006-01-02T15:04:05Z07:00"), used by
//     ts_parse / ts_format. It is not a directive language: each recognised
//     component is spelled with the corresponding piece of the reference
//     instant Mon Jan 2 15:04:05 MST 2006, and everything else is literal.
//   * strptime/strftime directives (%Y, %m, ...), used by ts_strptime and
//     ts_strftime. `%f` is the microsecond extension Python has and C does not.
//   * ISO 8601 durations (P3Y6M4DT12H30M5S), used by parse_duration_iso8601 and
//     the calendar-aware ts_add_iso8601 / ts_sub_iso8601.
//
// Calendar arithmetic goes through std::chrono's civil calendar rather than
// timegm/localtime, which are not thread-safe in the way a Seastar shard needs
// and would drag in a global TZ. IANA zone lookup uses std::chrono::locate_zone,
// which reads the system tzdata.
#include <algorithm>
#include <limits>
#include "swordfish/methods.hh"
#include "swordfish/value.hh"

#include "methods_util.hh"

#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>
#include <vector>

namespace sf {

namespace {

constexpr int64_t NS_PER_SEC = 1'000'000'000;

// Floor division, so instants before the epoch land on the right second.
int64_t floor_div(int64_t a, int64_t b) {
    const int64_t q = a / b;
    return (a % b != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
}
int64_t floor_mod(int64_t a, int64_t b) { return a - floor_div(a, b) * b; }

struct civil {
    int  year;
    unsigned month;   // 1-12
    unsigned day;     // 1-31
    int  hour, minute, second;
    int  nano;
    int  yday;        // 1-366
    int  wday;        // 0 = Sunday
};

// Splits an instant into calendar fields as seen from `offset`.
civil to_civil(int64_t unix_nanos, int32_t offset_sec) {
    using namespace std::chrono;
    const int64_t local_ns = unix_nanos + static_cast<int64_t>(offset_sec) * NS_PER_SEC;
    const int64_t secs = floor_div(local_ns, NS_PER_SEC);
    const int64_t nsec = floor_mod(local_ns, NS_PER_SEC);
    const int64_t days = floor_div(secs, 86400);
    const int64_t sod  = floor_mod(secs, 86400);
    const sys_days sd{std::chrono::days{days}};
    const year_month_day ymd{sd};
    const year_month_day jan1{ymd.year(), January, day{1}};
    civil c{};
    c.year   = static_cast<int>(ymd.year());
    c.month  = static_cast<unsigned>(ymd.month());
    c.day    = static_cast<unsigned>(ymd.day());
    c.hour   = static_cast<int>(sod / 3600);
    c.minute = static_cast<int>((sod / 60) % 60);
    c.second = static_cast<int>(sod % 60);
    c.nano   = static_cast<int>(nsec);
    c.yday   = static_cast<int>((sd - sys_days{jan1}).count()) + 1;
    c.wday   = static_cast<int>(weekday{sd}.c_encoding());
    return c;
}

// The inverse. Out-of-range fields normalise the way Go's time.Date does, so
// month 13 rolls into the next year.
// THE REPRESENTABLE RANGE, and why it is narrower than the reference's.
//
// `sf::value` carries an instant as one int64 nanosecond count, which spans
// 1677-09-21 to 2262-04-11. Go's time.Time keeps seconds and nanoseconds in
// separate fields and reaches far beyond that, so there are instants the
// reference handles and swordfish cannot.
//
// Every one of them is a NAMED ERROR here. It used to wrap silently, which is
// far worse than refusing: `"1600-01-01T00:00:00Z".ts_parse(...)` returned
// 2184-07-20 and `"2300-01-01T00:00:00Z"` returned 1715-06-13, with nothing
// logged. Widening the representation would mean growing `value` past the
// 16 bytes it asserts, on every value in every message, or bit-packing its
// header; a pipeline needing instants outside this range can reach for a `cpp:`
// block instead. See README.md.
constexpr int64_t TS_MAX_SEC =  9223372036;      // INT64_MAX / 1e9, floored
constexpr int64_t TS_MIN_SEC = -9223372036;

[[noreturn]] void ts_out_of_range(const std::string& what) {
    throw eval_error(
        "timestamp out of range: " + what + " is outside 1677-09-21T00:12:44Z .. "
        "2262-04-11T23:47:16Z, the range swordfish can represent (it carries an "
        "instant as a 64-bit nanosecond count). Use a `cpp:` block if you need "
        "instants beyond it");
}

// Builds a timestamp from whole seconds plus a nanosecond remainder, or refuses.
value make_ts(int64_t secs, int64_t nano, int32_t offset_sec, const std::string& what) {
    int64_t ns = 0;
    if (secs > TS_MAX_SEC || secs < TS_MIN_SEC ||
        __builtin_mul_overflow(secs, NS_PER_SEC, &ns) ||
        __builtin_add_overflow(ns, nano, &ns))
        ts_out_of_range(what);
    return value::ts(ns, offset_sec);
}

// The civil instant as WHOLE SECONDS since the epoch. Split out from from_civil
// so the nanosecond multiplication that can overflow happens in one checked
// place rather than at each call site.
int64_t from_civil_secs(int year, int month, int day, int hour, int minute, int second,
                        int32_t offset_sec) {
    using namespace std::chrono;
    // Normalise the month first: year_month_day cannot hold month 0 or 13.
    const int64_t m0 = static_cast<int64_t>(month) - 1;
    year += static_cast<int>(floor_div(m0, 12));
    month = static_cast<int>(floor_mod(m0, 12)) + 1;
    const sys_days sd = sys_days{year_month_day{
        std::chrono::year{year}, std::chrono::month{static_cast<unsigned>(month)},
        std::chrono::day{1}}} + std::chrono::days{day - 1};
    const int64_t secs = static_cast<int64_t>(sd.time_since_epoch().count()) * 86400 +
                         static_cast<int64_t>(hour) * 3600 +
                         static_cast<int64_t>(minute) * 60 + second;
    return secs - offset_sec;
}

int64_t from_civil(int year, int month, int day, int hour, int minute, int second,
                   int64_t nano, int32_t offset_sec) {
    return from_civil_secs(year, month, day, hour, minute, second, offset_sec) *
               NS_PER_SEC + nano;
}

const char* const MONTH_LONG[] = {"January","February","March","April","May","June",
                                  "July","August","September","October","November","December"};
const char* const DAY_LONG[] = {"Sunday","Monday","Tuesday","Wednesday","Thursday",
                                "Friday","Saturday"};

void pad2(std::string& out, int v) {
    if (v < 10) out += '0';
    out += std::to_string(v);
}
void padn(std::string& out, int64_t v, int width) {
    std::string s = std::to_string(v);
    while (static_cast<int>(s.size()) < width) s.insert(s.begin(), '0');
    out += s;
}

// "+05:30", "-0500", "Z" -- shaped by the layout that asked for it.
void write_offset(std::string& out, int32_t offset, bool colon, bool z_for_utc) {
    if (offset == 0 && z_for_utc) { out += 'Z'; return; }
    int32_t v = offset;
    out += v < 0 ? '-' : '+';
    if (v < 0) v = -v;
    pad2(out, v / 3600);
    if (colon) out += ':';
    pad2(out, (v / 60) % 60);
}

// Fractional seconds. `trim` is Go's `.999` behaviour: drop trailing zeros, and
// the point with them when nothing is left.
void write_fraction(std::string& out, int nano, int digits, bool trim) {
    // CLAMPED to nine, as Go's appendNano clamps it (`if n > 9 { n = 9 }`).
    // resize() GROWS a string when the count is larger, filling with NUL, and
    // the layout's digit count comes straight off a user string with no bound:
    // `ts_format("2006-01-02T15:04:05.000000000000000Z")` emitted six raw NUL
    // bytes between the ninth digit and the 'Z'. With a layout taken from
    // message data carrying 200 zeros it returned a 221-character string, 191 of
    // them NUL, where the reference returns 30.
    digits = std::min(digits, 9);
    std::string frac;
    padn(frac, nano, 9);
    frac.resize(static_cast<size_t>(std::max(0, digits)));
    if (trim) {
        while (!frac.empty() && frac.back() == '0') frac.pop_back();
        if (frac.empty()) return;
    }
    out += '.';
    out += frac;
}

} // namespace

std::string ts_rfc3339_nano(int64_t unix_nanos, int32_t offset_sec) {
    const civil c = to_civil(unix_nanos, offset_sec);
    std::string out;
    out.reserve(35);
    padn(out, c.year, 4);
    out += '-'; pad2(out, static_cast<int>(c.month));
    out += '-'; pad2(out, static_cast<int>(c.day));
    out += 'T'; pad2(out, c.hour);
    out += ':'; pad2(out, c.minute);
    out += ':'; pad2(out, c.second);
    write_fraction(out, c.nano, 9, /*trim=*/true);
    write_offset(out, offset_sec, /*colon=*/true, /*z_for_utc=*/true);
    return out;
}

namespace m {

namespace {

// ---- zone lookup --------------------------------------------------------------

int32_t zone_offset(std::string_view name, int64_t unix_nanos) {
    if (name.empty() || name == "UTC" || name == "utc" || name == "Z") return 0;
    try {
        const auto* z = (name == "Local" || name == "local")
            ? std::chrono::current_zone()
            : std::chrono::locate_zone(name);
        const auto tp = std::chrono::sys_seconds{
            std::chrono::seconds{floor_div(unix_nanos, NS_PER_SEC)}};
        return static_cast<int32_t>(z->get_info(tp).offset.count());
    } catch (const std::exception&) {
        throw eval_error("unknown time zone " + std::string(name));
    }
}

// ---- number and text scanning ---------------------------------------------------

bool scan_int(std::string_view s, size_t& i, int max_digits, int& out, int min_digits = 1) {
    const size_t start = i;
    int v = 0;
    while (i < s.size() && i - start < static_cast<size_t>(max_digits) &&
           s[i] >= '0' && s[i] <= '9') {
        v = v * 10 + (s[i] - '0');
        ++i;
    }
    if (i - start < static_cast<size_t>(min_digits)) return false;
    out = v;
    return true;
}

// Case-insensitive match against a name table, trying the full spelling before
// the abbreviation so "June" is not read as "Jun" with a stray "e" left over.
// Returns the 0-based index, or -1.
int scan_name(std::string_view s, size_t& i, const char* const* names, size_t n,
              size_t abbrev_len) {
    auto matches = [&](std::string_view full, size_t len) {
        if (s.size() - i < len) return false;
        for (size_t j = 0; j < len; ++j)
            if (std::tolower(static_cast<unsigned char>(s[i + j])) !=
                std::tolower(static_cast<unsigned char>(full[j]))) return false;
        return true;
    };
    for (size_t k = 0; k < n; ++k) {
        const std::string_view full(names[k]);
        if (matches(full, full.size())) { i += full.size(); return static_cast<int>(k); }
    }
    for (size_t k = 0; k < n; ++k) {
        const std::string_view full(names[k]);
        if (abbrev_len < full.size() && matches(full, abbrev_len)) {
            i += abbrev_len;
            return static_cast<int>(k);
        }
    }
    return -1;
}

// ---- Go reference layouts ---------------------------------------------------------

// The layout tokens, longest first so "2006" is not read as "2" then "006" and
// "01" is not read as "0" then "1".
enum class tok {
    none, year4, year2, month_num, month_num1, month_abbr, month_long,
    day_num, day_num1, day_space, hour24, hour12, hour12_1,
    min2, min1, sec2, sec1, pm_upper, pm_lower, wday_abbr, wday_long,
    yday3, yday1, frac_fixed, frac_trim,
    tz_num, tz_num_colon, tz_num_short, tz_z, tz_z_colon, tz_z_short, tz_abbr,
};

struct layout_token {
    tok         kind = tok::none;
    std::string literal;    // for tok::none
    int         digits = 0; // fractional second width
};

std::vector<layout_token> parse_layout(std::string_view l) {
    struct entry { std::string_view text; tok kind = tok::none; };
    // Order matters: the first match wins, so longer spellings come first.
    static const entry table[] = {
        {"January", tok::month_long}, {"Jan", tok::month_abbr},
        {"Monday", tok::wday_long},   {"Mon", tok::wday_abbr},
        {"2006", tok::year4},
        {"01", tok::month_num},
        {"002", tok::yday3},
        {"__2", tok::yday1},
        {"_2", tok::day_space},
        {"02", tok::day_num},
        {"15", tok::hour24},
        {"03", tok::hour12},
        {"04", tok::min2},
        {"05", tok::sec2},
        {"PM", tok::pm_upper}, {"pm", tok::pm_lower},
        {"-070000", tok::tz_num}, {"-07:00:00", tok::tz_num_colon},
        {"-0700", tok::tz_num},   {"-07:00", tok::tz_num_colon},
        {"-07", tok::tz_num_short},
        {"Z070000", tok::tz_z},   {"Z07:00:00", tok::tz_z_colon},
        {"Z0700", tok::tz_z},     {"Z07:00", tok::tz_z_colon},
        {"Z07", tok::tz_z_short},
        {"MST", tok::tz_abbr},
        {"06", tok::year2},
        {"1", tok::month_num1},
        {"2", tok::day_num1},
        {"3", tok::hour12_1},
        {"4", tok::min1},
        {"5", tok::sec1},
    };
    std::vector<layout_token> out;
    auto push_literal = [&](char c) {
        if (out.empty() || out.back().kind != tok::none) out.push_back({tok::none, {}, 0});
        out.back().literal += c;
    };
    for (size_t i = 0; i < l.size();) {
        // Fractional seconds: .000 / .999 with any number of digits, and the
        // comma spelling Go also accepts.
        if ((l[i] == '.' || l[i] == ',') && i + 1 < l.size() &&
            (l[i + 1] == '0' || l[i + 1] == '9')) {
            const char d = l[i + 1];
            size_t j = i + 1;
            while (j < l.size() && l[j] == d) ++j;
            out.push_back({d == '0' ? tok::frac_fixed : tok::frac_trim, {},
                           static_cast<int>(j - i - 1)});
            i = j;
            continue;
        }
        bool matched = false;
        for (const auto& e : table) {
            if (l.compare(i, e.text.size(), e.text) == 0) {
                out.push_back({e.kind, {}, 0});
                i += e.text.size();
                matched = true;
                break;
            }
        }
        if (!matched) push_literal(l[i++]);
    }
    return out;
}

std::string format_layout(const civil& c, int32_t offset, std::string_view layout) {
    std::string out;
    for (const auto& t : parse_layout(layout)) {
        switch (t.kind) {
        case tok::none:       out += t.literal; break;
        case tok::year4:      padn(out, c.year, 4); break;
        case tok::year2:      pad2(out, ((c.year % 100) + 100) % 100); break;
        case tok::month_num:  pad2(out, static_cast<int>(c.month)); break;
        case tok::month_num1: out += std::to_string(c.month); break;
        case tok::month_abbr: out.append(MONTH_LONG[c.month - 1], 3); break;
        case tok::month_long: out += MONTH_LONG[c.month - 1]; break;
        case tok::day_num:    pad2(out, static_cast<int>(c.day)); break;
        case tok::day_num1:   out += std::to_string(c.day); break;
        case tok::day_space:  if (c.day < 10) out += ' '; out += std::to_string(c.day); break;
        case tok::hour24:     pad2(out, c.hour); break;
        case tok::hour12:     pad2(out, c.hour % 12 == 0 ? 12 : c.hour % 12); break;
        case tok::hour12_1:   out += std::to_string(c.hour % 12 == 0 ? 12 : c.hour % 12); break;
        case tok::min2:       pad2(out, c.minute); break;
        case tok::min1:       out += std::to_string(c.minute); break;
        case tok::sec2:       pad2(out, c.second); break;
        case tok::sec1:       out += std::to_string(c.second); break;
        case tok::pm_upper:   out += c.hour < 12 ? "AM" : "PM"; break;
        case tok::pm_lower:   out += c.hour < 12 ? "am" : "pm"; break;
        case tok::wday_abbr:  out.append(DAY_LONG[c.wday], 3); break;
        case tok::wday_long:  out += DAY_LONG[c.wday]; break;
        case tok::yday3:      padn(out, c.yday, 3); break;
        case tok::yday1:      out += std::to_string(c.yday); break;
        case tok::frac_fixed: write_fraction(out, c.nano, t.digits, false); break;
        case tok::frac_trim:  write_fraction(out, c.nano, t.digits, true); break;
        case tok::tz_num:        write_offset(out, offset, false, false); break;
        case tok::tz_num_colon:  write_offset(out, offset, true,  false); break;
        case tok::tz_num_short:  {
            int32_t v = offset;
            out += v < 0 ? '-' : '+';
            if (v < 0) v = -v;
            pad2(out, v / 3600);
            break;
        }
        case tok::tz_z:          write_offset(out, offset, false, true); break;
        case tok::tz_z_colon:    write_offset(out, offset, true,  true); break;
        case tok::tz_z_short:
            if (offset == 0) { out += 'Z'; break; }
            {
                int32_t v = offset < 0 ? -offset : offset;
                out += offset < 0 ? '-' : '+';
                pad2(out, v / 3600);
            }
            break;
        case tok::tz_abbr:
            // A timestamp carries an offset, not a named zone. Go prints the
            // numeric form when a zone has no name, so that is what happens
            // here for everything but UTC.
            if (offset == 0) out += "UTC";
            else write_offset(out, offset, false, false);
            break;
        }
    }
    return out;
}

struct parsed_time {
    int year = 1, month = 1, day = 1;
    int hour = 0, minute = 0, second = 0;
    int nano = 0;
    int yday = 0;
    bool pm = false, has_ampm = false;
    bool has_offset = false;
    int32_t offset = 0;
};

[[noreturn]] void bad_time(std::string_view input, std::string_view layout) {
    throw eval_error("parsing time \"" + std::string(input) + "\" as \"" +
                     std::string(layout) + "\": cannot parse");
}

value parse_with_layout(std::string_view s, std::string_view layout) {
    parsed_time p;
    size_t i = 0;
    auto need = [&](bool ok) { if (!ok) bad_time(s, layout); };

    const auto toks = parse_layout(layout);

    // "When parsing (only), the input may contain a fractional second field
    // immediately after the seconds field, even if the layout does not signify
    // its presence." -- Go's own documentation, and its implementation checks
    // whether the NEXT layout chunk is a fractional-second token before doing
    // it (time/format.go, "Special case: do we have a fractional second but no
    // fractional second in the format?").
    //
    // Without this, parsing ordinary RFC 3339 data that carries milliseconds
    // with the plain `2006-01-02T15:04:05Z07:00` layout FAILED -- which is a
    // very ordinary thing to do, and the reference accepts it.
    auto bare_fraction_after_seconds = [&](size_t ti) {
        if (i + 1 >= s.size()) return;
        if (s[i] != '.' && s[i] != ',') return;
        if (!(s[i + 1] >= '0' && s[i + 1] <= '9')) return;
        if (ti + 1 < toks.size() && (toks[ti + 1].kind == tok::frac_fixed ||
                                     toks[ti + 1].kind == tok::frac_trim))
            return;                       // the layout asks for it; parse it there
        ++i;
        const size_t start = i;
        while (i < s.size() && s[i] >= '0' && s[i] <= '9') ++i;
        std::string digits(s.substr(start, i - start));
        digits.resize(9, '0');            // truncated to nanoseconds, as Go does
        p.nano = std::stoi(digits);
    };

    for (size_t ti = 0; ti < toks.size(); ++ti) {
        const auto& t = toks[ti];
        switch (t.kind) {
        case tok::none:
            for (char c : t.literal) {
                need(i < s.size() && s[i] == c);
                ++i;
            }
            break;
        case tok::year4:      need(scan_int(s, i, 4, p.year, 4)); break;
        case tok::year2: {
            int v = 0;
            need(scan_int(s, i, 2, v, 2));
            p.year = v >= 69 ? 1900 + v : 2000 + v;    // Go's pivot
            break;
        }
        case tok::month_num:  need(scan_int(s, i, 2, p.month, 2)); break;
        case tok::month_num1: need(scan_int(s, i, 2, p.month)); break;
        case tok::month_abbr:
        case tok::month_long: {
            // Both spellings share an abbreviation length: `Jan` for the
            // short form, and the long form matches in full before it is tried.
            const int k = scan_name(s, i, MONTH_LONG, 12, 3);
            need(k >= 0);
            p.month = k + 1;
            break;
        }
        case tok::day_num:    need(scan_int(s, i, 2, p.day, 2)); break;
        case tok::day_num1:   need(scan_int(s, i, 2, p.day)); break;
        case tok::day_space:
            if (i < s.size() && s[i] == ' ') ++i;
            need(scan_int(s, i, 2, p.day));
            break;
        case tok::hour24:     need(scan_int(s, i, 2, p.hour, 2)); break;
        case tok::hour12:     need(scan_int(s, i, 2, p.hour, 2)); break;
        case tok::hour12_1:   need(scan_int(s, i, 2, p.hour)); break;
        case tok::min2:       need(scan_int(s, i, 2, p.minute, 2)); break;
        case tok::min1:       need(scan_int(s, i, 2, p.minute)); break;
        case tok::sec2:
            need(scan_int(s, i, 2, p.second, 2));
            bare_fraction_after_seconds(ti);
            break;
        case tok::sec1:
            need(scan_int(s, i, 2, p.second));
            bare_fraction_after_seconds(ti);
            break;
        case tok::pm_upper:
        case tok::pm_lower: {
            need(i + 1 < s.size());
            const char c = static_cast<char>(std::tolower(static_cast<unsigned char>(s[i])));
            need((c == 'a' || c == 'p') &&
                 std::tolower(static_cast<unsigned char>(s[i + 1])) == 'm');
            p.pm = c == 'p';
            p.has_ampm = true;
            i += 2;
            break;
        }
        case tok::wday_abbr:
        case tok::wday_long: {
            // Parsed and discarded: Go ignores the weekday when building the
            // instant, and a mismatch is not an error.
            const int k = scan_name(s, i, DAY_LONG, 7, 3);
            need(k >= 0);
            break;
        }
        case tok::yday3:      need(scan_int(s, i, 3, p.yday, 3)); break;
        case tok::yday1:      need(scan_int(s, i, 3, p.yday)); break;
        case tok::frac_fixed:
        case tok::frac_trim: {
            // A `.999` fraction is optional in the input; a `.000` one is not.
            if (i >= s.size() || (s[i] != '.' && s[i] != ',')) {
                need(t.kind == tok::frac_trim);
                break;
            }
            ++i;
            const size_t start = i;
            while (i < s.size() && s[i] >= '0' && s[i] <= '9') ++i;
            need(i > start);
            std::string digits(s.substr(start, i - start));
            digits.resize(9, '0');
            p.nano = std::stoi(digits);
            break;
        }
        case tok::tz_num: case tok::tz_num_colon: case tok::tz_num_short:
        case tok::tz_z:   case tok::tz_z_colon:   case tok::tz_z_short: {
            const bool allows_z = t.kind == tok::tz_z || t.kind == tok::tz_z_colon ||
                                  t.kind == tok::tz_z_short;
            if (allows_z && i < s.size() && (s[i] == 'Z' || s[i] == 'z')) {
                ++i;
                p.has_offset = true;
                p.offset = 0;
                break;
            }
            need(i < s.size() && (s[i] == '+' || s[i] == '-'));
            const int sign = s[i] == '-' ? -1 : 1;
            ++i;
            int hh = 0, mm = 0;
            need(scan_int(s, i, 2, hh, 2));
            const bool shortf = t.kind == tok::tz_num_short || t.kind == tok::tz_z_short;
            if (!shortf) {
                if (i < s.size() && s[i] == ':') ++i;
                need(scan_int(s, i, 2, mm, 2));
            }
            p.has_offset = true;
            p.offset = sign * (hh * 3600 + mm * 60);
            break;
        }
        case tok::tz_abbr: {
            // Only the zone abbreviations that map to a fixed offset without a
            // location database are honoured; anything else is consumed and the
            // instant is treated as UTC, which is what Go does for an
            // abbreviation it cannot resolve.
            const size_t start = i;
            while (i < s.size() && std::isalpha(static_cast<unsigned char>(s[i]))) ++i;
            const std::string_view abbr = s.substr(start, i - start);
            need(!abbr.empty());
            if (abbr == "UTC" || abbr == "GMT" || abbr == "UT" || abbr == "Z") {
                p.has_offset = true;
                p.offset = 0;
            }
            break;
        }
        }
    }
    if (i != s.size()) bad_time(s, layout);

    if (p.has_ampm) {
        if (p.pm && p.hour < 12) p.hour += 12;
        if (!p.pm && p.hour == 12) p.hour = 0;
    }
    // A day-of-year layout supplies the day instead of month/day.
    if (p.yday > 0) {
        p.month = 1;
        p.day = p.yday;
    }
    return make_ts(from_civil_secs(p.year, p.month, p.day, p.hour, p.minute,
                                   p.second, p.offset),
                   p.nano, p.offset, "\"" + std::string(s) + "\"");
}

// ---- strftime / strptime -----------------------------------------------------------

std::string format_strftime(const civil& c, int32_t offset, std::string_view f) {
    std::string out;
    for (size_t i = 0; i < f.size(); ++i) {
        if (f[i] != '%' || i + 1 >= f.size()) { out += f[i]; continue; }
        switch (f[++i]) {
        case 'Y': padn(out, c.year, 4); break;
        case 'y': pad2(out, ((c.year % 100) + 100) % 100); break;
        case 'C': pad2(out, c.year / 100); break;
        case 'm': pad2(out, static_cast<int>(c.month)); break;
        case 'b': case 'h': out.append(MONTH_LONG[c.month - 1], 3); break;
        case 'B': out += MONTH_LONG[c.month - 1]; break;
        case 'd': pad2(out, static_cast<int>(c.day)); break;
        case 'e': if (c.day < 10) out += ' '; out += std::to_string(c.day); break;
        case 'j': padn(out, c.yday, 3); break;
        case 'H': pad2(out, c.hour); break;
        case 'I': pad2(out, c.hour % 12 == 0 ? 12 : c.hour % 12); break;
        case 'M': pad2(out, c.minute); break;
        case 'S': pad2(out, c.second); break;
        // Python's microsecond extension, which the reference documents.
        case 'f': padn(out, c.nano / 1000, 6); break;
        case 'p': out += c.hour < 12 ? "AM" : "PM"; break;
        case 'P': out += c.hour < 12 ? "am" : "pm"; break;
        case 'a': out.append(DAY_LONG[c.wday], 3); break;
        case 'A': out += DAY_LONG[c.wday]; break;
        case 'u': out += std::to_string(c.wday == 0 ? 7 : c.wday); break;
        case 'w': out += std::to_string(c.wday); break;
        case 'z': write_offset(out, offset, false, false); break;
        case 'Z': if (offset == 0) out += "UTC"; else write_offset(out, offset, false, false); break;
        case 's': out += std::to_string(
                      from_civil(c.year, static_cast<int>(c.month), static_cast<int>(c.day),
                                 c.hour, c.minute, c.second, 0, offset) / NS_PER_SEC);
                  break;
        case 'n': out += '\n'; break;
        case 't': out += '\t'; break;
        case '%': out += '%'; break;
        case 'D': padn(out, static_cast<int>(c.month), 2); out += '/';
                  padn(out, static_cast<int>(c.day), 2); out += '/';
                  pad2(out, ((c.year % 100) + 100) % 100); break;
        case 'F': padn(out, c.year, 4); out += '-'; pad2(out, static_cast<int>(c.month));
                  out += '-'; pad2(out, static_cast<int>(c.day)); break;
        case 'T': pad2(out, c.hour); out += ':'; pad2(out, c.minute);
                  out += ':'; pad2(out, c.second); break;
        case 'R': pad2(out, c.hour); out += ':'; pad2(out, c.minute); break;
        default:
            // An unknown directive is a mistake in the config, not something to
            // pass through silently: a `%Q` that emitted "%Q" would look like
            // working output.
            throw eval_error(std::string("unsupported strftime directive %") + f[i]);
        }
    }
    return out;
}

value parse_strptime(std::string_view s, std::string_view f) {
    parsed_time p;
    size_t i = 0;
    auto need = [&](bool ok) { if (!ok) bad_time(s, f); };

    for (size_t k = 0; k < f.size(); ++k) {
        if (f[k] != '%') {
            if (f[k] == ' ') {                       // whitespace is flexible
                while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) ++i;
                continue;
            }
            need(i < s.size() && s[i] == f[k]);
            ++i;
            continue;
        }
        need(k + 1 < f.size());
        switch (f[++k]) {
        case 'Y': need(scan_int(s, i, 4, p.year, 4)); break;
        case 'y': { int v = 0; need(scan_int(s, i, 2, v, 2));
                    p.year = v >= 69 ? 1900 + v : 2000 + v; break; }
        case 'm': need(scan_int(s, i, 2, p.month)); break;
        case 'b': case 'h': case 'B': {
            const int idx = scan_name(s, i, MONTH_LONG, 12, 3);
            need(idx >= 0);
            p.month = idx + 1;
            break;
        }
        case 'd': case 'e':
            while (i < s.size() && s[i] == ' ') ++i;
            need(scan_int(s, i, 2, p.day));
            break;
        case 'j': need(scan_int(s, i, 3, p.yday)); break;
        case 'H': need(scan_int(s, i, 2, p.hour)); break;
        case 'I': need(scan_int(s, i, 2, p.hour)); break;
        case 'M': need(scan_int(s, i, 2, p.minute)); break;
        case 'S': need(scan_int(s, i, 2, p.second)); break;
        case 'f': {
            const size_t start = i;
            while (i < s.size() && s[i] >= '0' && s[i] <= '9') ++i;
            need(i > start);
            std::string digits(s.substr(start, i - start));
            digits.resize(9, '0');
            p.nano = std::stoi(digits);
            break;
        }
        case 'p': case 'P': {
            need(i + 1 < s.size());
            const char c = static_cast<char>(std::tolower(static_cast<unsigned char>(s[i])));
            need((c == 'a' || c == 'p') &&
                 std::tolower(static_cast<unsigned char>(s[i + 1])) == 'm');
            p.pm = c == 'p';
            p.has_ampm = true;
            i += 2;
            break;
        }
        case 'a': case 'A': need(scan_name(s, i, DAY_LONG, 7, 3) >= 0); break;
        case 'z': {
            if (i < s.size() && (s[i] == 'Z' || s[i] == 'z')) {
                ++i; p.has_offset = true; p.offset = 0; break;
            }
            need(i < s.size() && (s[i] == '+' || s[i] == '-'));
            const int sign = s[i] == '-' ? -1 : 1;
            ++i;
            int hh = 0, mm = 0;
            need(scan_int(s, i, 2, hh, 2));
            if (i < s.size() && s[i] == ':') ++i;
            if (i < s.size() && std::isdigit(static_cast<unsigned char>(s[i])))
                need(scan_int(s, i, 2, mm, 2));
            p.has_offset = true;
            p.offset = sign * (hh * 3600 + mm * 60);
            break;
        }
        case 'Z': {
            const size_t start = i;
            while (i < s.size() && std::isalpha(static_cast<unsigned char>(s[i]))) ++i;
            const std::string_view abbr = s.substr(start, i - start);
            need(!abbr.empty());
            if (abbr == "UTC" || abbr == "GMT" || abbr == "UT" || abbr == "Z") {
                p.has_offset = true;
                p.offset = 0;
            }
            break;
        }
        case 's': {
            const size_t start = i;
            if (i < s.size() && (s[i] == '-' || s[i] == '+')) ++i;
            while (i < s.size() && std::isdigit(static_cast<unsigned char>(s[i]))) ++i;
            need(i > start);
            // std::stoll THROWS std::out_of_range past int64 -- a raw library
            // exception, not an eval_error, so the pipeline counted a
            // proc_error whose whole text was the word "stoll". The scan loop
            // above accepts any number of digits, so message data reaches it
            // directly.
            int64_t secs = 0;
            try {
                secs = std::stoll(std::string(s.substr(start, i - start)));
            } catch (const std::exception&) {
                need(false);   // the layout's own "unparsed string" error
            }
            return make_ts(secs, 0, 0, std::to_string(secs) + "s since the epoch");
        }
        case 'n': case 't':
            while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) ++i;
            break;
        case '%': need(i < s.size() && s[i] == '%'); ++i; break;
        default:
            throw eval_error(std::string("unsupported strptime directive %") + f[k]);
        }
    }
    if (i != s.size()) bad_time(s, f);
    if (p.has_ampm) {
        if (p.pm && p.hour < 12) p.hour += 12;
        if (!p.pm && p.hour == 12) p.hour = 0;
    }
    if (p.yday > 0) { p.month = 1; p.day = p.yday; }
    return make_ts(from_civil_secs(p.year, p.month, p.day, p.hour, p.minute, p.second,
                                   p.offset),
                   p.nano, p.offset, "\"" + std::string(s) + "\"");
}

// ---- coercion to a timestamp -------------------------------------------------------

// A ts_* method accepts an actual timestamp, a unix time as a number (with
// optional decimals), or an RFC 3339 string, which is what the reference
// documents for all of them.
value coerce_ts(const value& v) {
    if (v.is_ts()) return v;
    if (v.type() == vtype::i64)
        return make_ts(v.as_i64(), 0, 0, std::to_string(v.as_i64()) + "s since the epoch");
    if (v.is_float()) {
        const double d = v.as_f64();
        // Checked on the DOUBLE first: casting an out-of-range double to int64
        // is undefined behaviour rather than a wrap, so the range test cannot
        // come after the cast.
        if (!std::isfinite(d) || d > static_cast<double>(TS_MAX_SEC) ||
            d < static_cast<double>(TS_MIN_SEC))
            ts_out_of_range(std::to_string(d) + "s since the epoch");
        const double whole = std::trunc(d);
        const int64_t secs = static_cast<int64_t>(whole);
        const int64_t nano = static_cast<int64_t>(std::llround((d - whole) * 1e9));
        return make_ts(secs, nano, 0, std::to_string(d) + "s since the epoch");
    }
    if (v.is_stringy()) {
        // RFC 3339, with or without a fraction, as time.Parse(time.RFC3339)
        // accepts it.
        return parse_with_layout(v.as_string(), "2006-01-02T15:04:05.999999999Z07:00");
    }
    wrong("timestamp", v);
}

// ---- durations ----------------------------------------------------------------------

// n units of `unit_ns`, kept exact when n is a whole number. Multiplying in
// double loses precision above 2^53 nanoseconds, which is only 104 days.
//
// Returns false when the result does not fit in an int64 nanosecond count,
// rather than producing a number. Both arms could overflow silently: the exact
// arm multiplied without a check, so `2562048h` came back as the NEGATIVE
// duration -9223371273709551616 where the reference reports
// `time: invalid duration "2562048h"`; and casting an out-of-range double to
// int64 is undefined behaviour, not a wrap.
bool scale_ns(double n, int64_t unit_ns, int64_t& out) {
    if (!std::isfinite(n)) return false;
    if (n == std::trunc(n) && std::fabs(n) < 4e9)
        return !__builtin_mul_overflow(static_cast<int64_t>(n), unit_ns, &out);
    const double prod = n * static_cast<double>(unit_ns);
    // Strictly less than 2^63 as a double, so the cast below is defined. The
    // comparison is written this way round so a NaN product fails it.
    if (!(std::fabs(prod) < 9.2233720368547758e18)) return false;
    out = static_cast<int64_t>(std::llround(prod));
    return true;
}

int64_t parse_go_duration(std::string_view s) {
    if (s.empty()) throw eval_error("invalid duration: empty string");
    size_t i = 0;
    int sign = 1;
    if (s[i] == '+' || s[i] == '-') { sign = s[i] == '-' ? -1 : 1; ++i; }
    if (s.substr(i) == "0") return 0;
    if (i == s.size()) throw eval_error("invalid duration: " + std::string(s));
    int64_t total = 0;
    bool saw_component = false;
    while (i < s.size()) {
        const size_t start = i;
        while (i < s.size() && ((s[i] >= '0' && s[i] <= '9') || s[i] == '.')) ++i;
        if (i == start) throw eval_error("invalid duration: " + std::string(s));
        const double n = std::stod(std::string(s.substr(start, i - start)));
        const size_t ustart = i;
        while (i < s.size() && !((s[i] >= '0' && s[i] <= '9') || s[i] == '.')) ++i;
        const std::string_view unit = s.substr(ustart, i - ustart);
        int64_t mult = 0;
        if      (unit == "ns")                    mult = 1;
        else if (unit == "us" || unit == "µs" || unit == "μs") mult = 1'000;
        else if (unit == "ms")                    mult = 1'000'000;
        else if (unit == "s")                     mult = NS_PER_SEC;
        else if (unit == "m")                     mult = 60 * NS_PER_SEC;
        else if (unit == "h")                     mult = 3600 * NS_PER_SEC;
        else throw eval_error("unknown unit \"" + std::string(unit) + "\" in duration \"" +
                              std::string(s) + "\"");
        int64_t part = 0;
        if (!scale_ns(n, mult, part) || __builtin_add_overflow(total, part, &total))
            throw eval_error("invalid duration: " + std::string(s) +
                             " (out of range for a 64-bit nanosecond count)");
        saw_component = true;
    }
    if (!saw_component) throw eval_error("invalid duration: " + std::string(s));
    // Negating INT64_MIN is undefined, and it is reachable: the accumulation
    // above can land exactly there.
    if (sign < 0 && total == std::numeric_limits<int64_t>::min())
        throw eval_error("invalid duration: " + std::string(s) +
                         " (out of range for a 64-bit nanosecond count)");
    return sign * total;
}

struct iso_duration {
    // All doubles: ISO 8601 permits a fraction on any component. The calendar
    // shift truncates years and months, because AddDate takes integers.
    double years = 0, months = 0, days = 0;
    double hours = 0, minutes = 0, seconds = 0;
};


iso_duration parse_iso_duration(std::string_view s) {
    if (s.empty() || s[0] != 'P')
        throw eval_error("invalid ISO 8601 duration: " + std::string(s));
    iso_duration d{};
    bool in_time = false;
    size_t i = 1;
    while (i < s.size()) {
        if (s[i] == 'T') { in_time = true; ++i; continue; }
        const size_t start = i;
        while (i < s.size() && ((s[i] >= '0' && s[i] <= '9') || s[i] == '.' || s[i] == ',')) ++i;
        if (i == start || i >= s.size())
            throw eval_error("invalid ISO 8601 duration: " + std::string(s));
        std::string num(s.substr(start, i - start));
        for (auto& c : num) if (c == ',') c = '.';
        // std::stod throws std::out_of_range -- a RAW library exception, not an
        // eval_error -- for a literal outside double's range, and the value is
        // bounded afterwards because a finite but astronomical component
        // overflowed silently downstream: `P99999999999999999999Y` produced
        // INT64_MIN where the reference reports `number invalid or out of
        // range`.
        double n = 0;
        try {
            n = std::stod(num);
        } catch (const std::exception&) {
            throw eval_error("number invalid or out of range in ISO 8601 duration: " +
                             std::string(s));
        }
        if (!std::isfinite(n) || std::fabs(n) > 1e15)
            throw eval_error("number invalid or out of range in ISO 8601 duration: " +
                             std::string(s));
        switch (s[i++]) {
        case 'Y': d.years += n; break;
        case 'W': d.days  += n * 7; break;
        case 'D': d.days  += n; break;
        case 'H': if (!in_time) throw eval_error("H must follow T in an ISO 8601 duration");
                  d.hours += n; break;
        case 'S': if (!in_time) throw eval_error("S must follow T in an ISO 8601 duration");
                  d.seconds += n; break;
        case 'M':
            // M means months before the T and minutes after it. Getting this
            // backwards silently turns "PT5M" into five months.
            if (in_time) d.minutes += n; else d.months += n;
            break;
        default:
            throw eval_error("invalid ISO 8601 duration: " + std::string(s));
        }
    }
    return d;
}

// Calendar-aware shift: years and months move the civil date, the rest is a
// fixed number of nanoseconds. That is what makes adding P1M to 31 January land
// on 3 March rather than 30 days later.
value shift_iso(const value& ts, std::string_view spec, int sign) {
    const iso_duration d = parse_iso_duration(spec);
    const int32_t off = ts.ts_offset();
    civil c = to_civil(ts.ts_nanos(), off);
    // The component bound in parse_iso_duration keeps these casts in range, and
    // the additions are checked so a combination of two in-range components
    // cannot walk out of it.
    int64_t months = 0;
    if (__builtin_mul_overflow(static_cast<int64_t>(d.years), int64_t{12}, &months) ||
        __builtin_add_overflow(months, static_cast<int64_t>(d.months), &months) ||
        __builtin_mul_overflow(months, static_cast<int64_t>(sign), &months))
        throw eval_error("ISO 8601 duration is out of range: " + std::string(spec));
    int64_t total_month = 0;
    if (__builtin_mul_overflow(static_cast<int64_t>(c.year), int64_t{12}, &total_month) ||
        __builtin_add_overflow(total_month, static_cast<int64_t>(c.month) - 1, &total_month) ||
        __builtin_add_overflow(total_month, months, &total_month))
        throw eval_error("ISO 8601 duration is out of range: " + std::string(spec));
    const int y = static_cast<int>(floor_div(total_month, 12));
    const int mo = static_cast<int>(floor_mod(total_month, 12)) + 1;
    // Through the checked constructor: a calendar shift can walk the instant out
    // of the representable range, and `from_civil` would have wrapped it.
    const value shifted = make_ts(
        from_civil_secs(y, mo, static_cast<int>(c.day), c.hour, c.minute, c.second, off),
        c.nano, off, "the result of shifting by " + std::string(spec));
    int64_t base = shifted.ts_nanos();
    // Checked, all of it. `ts_add_iso8601("P99999999999999999999Y")` used to
    // return the timestamp UNCHANGED -- a silent no-op on an explicit add --
    // because the overflow cancelled out; the reference errors.
    int64_t rest = 0;
    for (const auto& [n, unit] : {std::pair<double, int64_t>{d.days, 86400 * NS_PER_SEC},
                                  {d.hours,   3600 * NS_PER_SEC},
                                  {d.minutes,   60 * NS_PER_SEC},
                                  {d.seconds,        NS_PER_SEC}}) {
        int64_t part = 0;
        if (!scale_ns(n, unit, part) || __builtin_add_overflow(rest, part, &rest))
            throw eval_error("ISO 8601 duration is out of range: " + std::string(spec));
    }
    if (sign < 0 && rest == std::numeric_limits<int64_t>::min())
        throw eval_error("ISO 8601 duration is out of range: " + std::string(spec));
    if (__builtin_add_overflow(base, static_cast<int64_t>(sign) * rest, &base))
        throw eval_error("ISO 8601 duration is out of range: " + std::string(spec));
    return value::ts(base, off);
}

std::string want_layout(const value& v, const char* dflt) {
    return v.is_stringy() ? v.as_string() : std::string(dflt);
}

} // namespace

// ---- the methods --------------------------------------------------------------------

value to_timestamp(const value& v) { return coerce_ts(v); }

value ts_format(const value& v, const value& layout, const value& tz) {
    value t = coerce_ts(v);
    int32_t off = t.ts_offset();
    if (tz.is_stringy()) off = zone_offset(tz.as_string(), t.ts_nanos());
    return value(format_layout(to_civil(t.ts_nanos(), off), off,
                               want_layout(layout, "2006-01-02T15:04:05.999999999Z07:00")));
}

value ts_strftime(const value& v, const value& directives, const value& tz) {
    value t = coerce_ts(v);
    int32_t off = t.ts_offset();
    if (tz.is_stringy()) off = zone_offset(tz.as_string(), t.ts_nanos());
    return value(format_strftime(to_civil(t.ts_nanos(), off), off, want_string(directives)));
}

value ts_parse(const value& v, const value& layout) {
    return parse_with_layout(want_string(v), want_string(layout));
}

value ts_strptime(const value& v, const value& directives) {
    return parse_strptime(want_string(v), want_string(directives));
}

value ts_tz(const value& v, const value& tz) {
    const value t = coerce_ts(v);
    return value::ts(t.ts_nanos(), zone_offset(want_string(tz), t.ts_nanos()));
}

value ts_unix(const value& v) {
    return value(floor_div(coerce_ts(v).ts_nanos(), NS_PER_SEC));
}
value ts_unix_milli(const value& v) {
    return value(floor_div(coerce_ts(v).ts_nanos(), 1'000'000));
}
value ts_unix_micro(const value& v) {
    return value(floor_div(coerce_ts(v).ts_nanos(), 1'000));
}
value ts_unix_nano(const value& v) { return value(coerce_ts(v).ts_nanos()); }

value ts_sub(const value& v, const value& other) {
    return value(coerce_ts(v).ts_nanos() - coerce_ts(other).ts_nanos());
}

value ts_round(const value& v, const value& duration) {
    const value t = coerce_ts(v);
    if (!duration.is_number()) wrong("number", duration);
    const int64_t d = duration.as_i64();
    if (d <= 0) throw eval_error("ts_round requires a positive duration");
    const int64_t n = t.ts_nanos();
    // Halfway rounds up, as Go's Time.Round does. Written as `rem >= d - rem`
    // rather than `rem * 2 >= d`, which overflows once d passes half of int64.
    const int64_t rem = floor_mod(n, d);
    const int64_t base = n - rem;
    return value::ts(rem >= d - rem ? base + d : base, t.ts_offset());
}

value ts_add_iso8601(const value& v, const value& spec) {
    return shift_iso(coerce_ts(v), want_string(spec), +1);
}
value ts_sub_iso8601(const value& v, const value& spec) {
    return shift_iso(coerce_ts(v), want_string(spec), -1);
}

value parse_duration(const value& v) {
    return value(parse_go_duration(want_string(v)));
}

value parse_duration_iso8601(const value& v) {
    const iso_duration d = parse_iso_duration(want_string(v));
    // Without an anchor instant a year and a month have no exact length, so
    // fixed averages stand in: the MEAN GREGORIAN year of 365.2425 days
    // (31556952 s) and a twelfth of it (2629746 s). The documented
    // P3Y6M4DT12H30M5S -> 110839937000000000 pins both -- 365.25 and 30.44,
    // the obvious guesses, are each off by thousands of seconds.
    int64_t total = 0;
    for (const auto& [n, unit] :
         {std::pair<double, int64_t>{d.years, 31556952 * NS_PER_SEC},
          {d.months, 2629746 * NS_PER_SEC},
          {d.days,      86400 * NS_PER_SEC},
          {d.hours,      3600 * NS_PER_SEC},
          {d.minutes,      60 * NS_PER_SEC},
          {d.seconds,           NS_PER_SEC}}) {
        int64_t part = 0;
        if (!scale_ns(n, unit, part) || __builtin_add_overflow(total, part, &total))
            throw eval_error("number invalid or out of range in ISO 8601 duration: " +
                             want_string(v));
    }
    return value(total);
}

} // namespace m

namespace fn {
value now() {
    const auto n = std::chrono::system_clock::now().time_since_epoch();
    return value::ts(std::chrono::duration_cast<std::chrono::nanoseconds>(n).count(), 0);
}
value timestamp_unix()       { return value(m::ts_unix(now())); }
value timestamp_unix_milli() { return value(m::ts_unix_milli(now())); }
value timestamp_unix_micro() { return value(m::ts_unix_micro(now())); }
value timestamp_unix_nano()  { return value(m::ts_unix_nano(now())); }
} // namespace fn

} // namespace sf
