// Bloblang string methods beyond the first set in methods.cc.
//
// Semantics follow the Go standard-library functions the reference
// implementation calls -- html.EscapeString, url.QueryEscape, filepath.Join,
// strconv.Unquote -- because those choices are observable: url.PathEscape
// leaves `&` alone where url.QueryEscape percent-encodes it, and a plausible
// "escape everything unreserved" reading gets both wrong.
#include "swordfish/methods.hh"
#include "swordfish/value.hh"

#include "encoding_util.hh"
#include "methods_util.hh"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace sf::m {

namespace {

// ---- UTF-8 -----------------------------------------------------------------

using enc::append_utf8;
using enc::next_rune;

// ---- HTML ------------------------------------------------------------------

// html.EscapeString escapes exactly these five, and uses the NUMERIC forms for
// the quotes -- &#39; and &#34;, not &apos; and &quot;. The documented examples
// depend on that.
const char* html_escape_of(char c) {
    switch (c) {
    case '&':  return "&amp;";
    case '\'': return "&#39;";
    case '<':  return "&lt;";
    case '>':  return "&gt;";
    case '"':  return "&#34;";
    default:   return nullptr;
    }
}

// The HTML 4 named character references plus the HTML5 additions Go's
// html.UnescapeString recognises for the same names. Go carries the full HTML5
// table of ~2000 names; an entity outside this set is left verbatim, which is
// also what Go does for a name it does not know -- so the divergence is
// confined to rare named references rather than producing wrong text.
const std::unordered_map<std::string_view, uint32_t>& html_entities() {
    static const std::unordered_map<std::string_view, uint32_t> t = {
        {"quot",34},{"amp",38},{"apos",39},{"lt",60},{"gt",62},{"nbsp",160},
        {"iexcl",161},{"cent",162},{"pound",163},{"curren",164},{"yen",165},
        {"brvbar",166},{"sect",167},{"uml",168},{"copy",169},{"ordf",170},
        {"laquo",171},{"not",172},{"shy",173},{"reg",174},{"macr",175},
        {"deg",176},{"plusmn",177},{"sup2",178},{"sup3",179},{"acute",180},
        {"micro",181},{"para",182},{"middot",183},{"cedil",184},{"sup1",185},
        {"ordm",186},{"raquo",187},{"frac14",188},{"frac12",189},{"frac34",190},
        {"iquest",191},{"Agrave",192},{"Aacute",193},{"Acirc",194},{"Atilde",195},
        {"Auml",196},{"Aring",197},{"AElig",198},{"Ccedil",199},{"Egrave",200},
        {"Eacute",201},{"Ecirc",202},{"Euml",203},{"Igrave",204},{"Iacute",205},
        {"Icirc",206},{"Iuml",207},{"ETH",208},{"Ntilde",209},{"Ograve",210},
        {"Oacute",211},{"Ocirc",212},{"Otilde",213},{"Ouml",214},{"times",215},
        {"Oslash",216},{"Ugrave",217},{"Uacute",218},{"Ucirc",219},{"Uuml",220},
        {"Yacute",221},{"THORN",222},{"szlig",223},{"agrave",224},{"aacute",225},
        {"acirc",226},{"atilde",227},{"auml",228},{"aring",229},{"aelig",230},
        {"ccedil",231},{"egrave",232},{"eacute",233},{"ecirc",234},{"euml",235},
        {"igrave",236},{"iacute",237},{"icirc",238},{"iuml",239},{"eth",240},
        {"ntilde",241},{"ograve",242},{"oacute",243},{"ocirc",244},{"otilde",245},
        {"ouml",246},{"divide",247},{"oslash",248},{"ugrave",249},{"uacute",250},
        {"ucirc",251},{"uuml",252},{"yacute",253},{"thorn",254},{"yuml",255},
        {"OElig",338},{"oelig",339},{"Scaron",352},{"scaron",353},{"Yuml",376},
        {"fnof",402},{"circ",710},{"tilde",732},
        {"Alpha",913},{"Beta",914},{"Gamma",915},{"Delta",916},{"Epsilon",917},
        {"Zeta",918},{"Eta",919},{"Theta",920},{"Iota",921},{"Kappa",922},
        {"Lambda",923},{"Mu",924},{"Nu",925},{"Xi",926},{"Omicron",927},
        {"Pi",928},{"Rho",929},{"Sigma",931},{"Tau",932},{"Upsilon",933},
        {"Phi",934},{"Chi",935},{"Psi",936},{"Omega",937},
        {"alpha",945},{"beta",946},{"gamma",947},{"delta",948},{"epsilon",949},
        {"zeta",950},{"eta",951},{"theta",952},{"iota",953},{"kappa",954},
        {"lambda",955},{"mu",956},{"nu",957},{"xi",958},{"omicron",959},
        {"pi",960},{"rho",961},{"sigmaf",962},{"sigma",963},{"tau",964},
        {"upsilon",965},{"phi",966},{"chi",967},{"psi",968},{"omega",969},
        {"thetasym",977},{"upsih",978},{"piv",982},
        {"ensp",8194},{"emsp",8195},{"thinsp",8201},{"zwnj",8204},{"zwj",8205},
        {"lrm",8206},{"rlm",8207},{"ndash",8211},{"mdash",8212},{"lsquo",8216},
        {"rsquo",8217},{"sbquo",8218},{"ldquo",8220},{"rdquo",8221},{"bdquo",8222},
        {"dagger",8224},{"Dagger",8225},{"bull",8226},{"hellip",8230},
        {"permil",8240},{"prime",8242},{"Prime",8243},{"lsaquo",8249},
        {"rsaquo",8250},{"oline",8254},{"frasl",8260},{"euro",8364},
        {"image",8465},{"weierp",8472},{"real",8476},{"trade",8482},
        {"alefsym",8501},{"larr",8592},{"uarr",8593},{"rarr",8594},{"darr",8595},
        {"harr",8596},{"crarr",8629},{"lArr",8656},{"uArr",8657},{"rArr",8658},
        {"dArr",8659},{"hArr",8660},{"forall",8704},{"part",8706},{"exist",8707},
        {"empty",8709},{"nabla",8711},{"isin",8712},{"notin",8713},{"ni",8715},
        {"prod",8719},{"sum",8721},{"minus",8722},{"lowast",8727},{"radic",8730},
        {"prop",8733},{"infin",8734},{"ang",8736},{"and",8743},{"or",8744},
        {"cap",8745},{"cup",8746},{"int",8747},{"there4",8756},{"sim",8764},
        {"cong",8773},{"asymp",8776},{"ne",8800},{"equiv",8801},{"le",8804},
        {"ge",8805},{"sub",8834},{"sup",8835},{"nsub",8836},{"sube",8838},
        {"supe",8839},{"oplus",8853},{"otimes",8855},{"perp",8869},{"sdot",8901},
        {"lceil",8968},{"rceil",8969},{"lfloor",8970},{"rfloor",8971},
        {"lang",9001},{"rang",9002},{"loz",9674},{"spades",9824},{"clubs",9827},
        {"hearts",9829},{"diams",9830},
    };
    return t;
}

// ---- percent-encoding ------------------------------------------------------

enum class url_mode { query, path_segment };

bool url_should_escape(unsigned char c, url_mode mode) {
    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'))
        return false;
    switch (c) {
    case '-': case '_': case '.': case '~':
        return false;
    // RFC 3986 §2.2 reserved. Whether each is escaped depends on the component
    // being built, which is why Go has a mode here at all: a path segment may
    // contain `&` and `=` unescaped, a query component may not.
    case '$': case '&': case '+': case ',': case '/': case ':':
    case ';': case '=': case '?': case '@':
        if (mode == url_mode::query) return true;
        return c == '/' || c == ';' || c == ',' || c == '?';
    default:
        return true;
    }
}

std::string url_escape(std::string_view s, url_mode mode) {
    static const char* H = "0123456789ABCDEF";
    std::string out;
    out.reserve(s.size());
    for (char ch : s) {
        const auto c = static_cast<unsigned char>(ch);
        if (mode == url_mode::query && c == ' ') { out += '+'; continue; }
        if (!url_should_escape(c, mode)) { out += ch; continue; }
        out += '%';
        out += H[c >> 4];
        out += H[c & 15];
    }
    return out;
}

// plus_is_space distinguishes url.QueryUnescape from url.PathUnescape: only the
// query form treats `+` as a space, and a path unescape that did so would
// corrupt any path containing a literal plus.
std::string url_unescape(std::string_view s, bool plus_is_space) {
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '+' && plus_is_space) { out += ' '; continue; }
        if (s[i] != '%') { out += s[i]; continue; }
        if (i + 2 >= s.size())
            throw eval_error("invalid URL escape: " + std::string(s.substr(i)));
        const int hi = enc::hex_nibble(s[i + 1]), lo = enc::hex_nibble(s[i + 2]);
        if (hi < 0 || lo < 0)
            throw eval_error("invalid URL escape: " + std::string(s.substr(i, 3)));
        out += static_cast<char>(hi * 16 + lo);
        i += 2;
    }
    return out;
}

// ---- file paths ------------------------------------------------------------

// path/filepath.Clean, for the Linux separator. Lexical only: it never touches
// the filesystem, so it cannot resolve symlinks, and neither does Go's.
std::string path_clean(std::string_view p) {
    if (p.empty()) return ".";
    const bool rooted = p[0] == '/';
    std::vector<std::string_view> parts;
    size_t i = 0;
    while (i < p.size()) {
        while (i < p.size() && p[i] == '/') ++i;
        const size_t start = i;
        while (i < p.size() && p[i] != '/') ++i;
        if (start == i) break;
        const std::string_view seg = p.substr(start, i - start);
        if (seg == ".") continue;
        if (seg == "..") {
            if (!parts.empty() && parts.back() != "..") { parts.pop_back(); continue; }
            if (rooted) continue;                       // /.. is /
            parts.push_back(seg);
            continue;
        }
        parts.push_back(seg);
    }
    std::string out(rooted ? "/" : "");
    for (size_t k = 0; k < parts.size(); ++k) {
        if (k) out += '/';
        out.append(parts[k]);
    }
    if (out.empty()) return ".";
    return out;
}

// ---- strconv.Unquote -------------------------------------------------------

// Reads one escape sequence after the backslash at s[i] and appends its value.
// `quote` is the delimiter, so \" is only valid inside a double-quoted literal.
void unquote_escape(std::string_view s, size_t& i, char quote, std::string& out) {
    if (i >= s.size()) throw eval_error("invalid syntax");
    const char c = s[i++];
    switch (c) {
    case 'a': out += '\a'; return;
    case 'b': out += '\b'; return;
    case 'f': out += '\f'; return;
    case 'n': out += '\n'; return;
    case 'r': out += '\r'; return;
    case 't': out += '\t'; return;
    case 'v': out += '\v'; return;
    case '\\': out += '\\'; return;
    case '\'': case '"':
        if (c != quote) throw eval_error("invalid syntax");
        out += c;
        return;
    case 'x': case 'u': case 'U': {
        const size_t n = c == 'x' ? 2 : (c == 'u' ? 4 : 8);
        if (i + n > s.size()) throw eval_error("invalid syntax");
        uint32_t cp = 0;
        for (size_t k = 0; k < n; ++k) {
            const int d = enc::hex_nibble(s[i + k]);
            if (d < 0) throw eval_error("invalid syntax");
            cp = cp * 16 + static_cast<uint32_t>(d);
        }
        i += n;
        // \x is a raw BYTE; \u and \U are code points.
        if (c == 'x') out += static_cast<char>(cp); else append_utf8(out, cp);
        return;
    }
    default:
        if (c >= '0' && c <= '7') {
            if (i + 1 >= s.size()) throw eval_error("invalid syntax");
            uint32_t v = static_cast<uint32_t>(c - '0');
            for (int k = 0; k < 2; ++k) {
                if (s[i] < '0' || s[i] > '7') throw eval_error("invalid syntax");
                v = v * 8 + static_cast<uint32_t>(s[i++] - '0');
            }
            if (v > 255) throw eval_error("invalid syntax");
            out += static_cast<char>(v);
            return;
        }
        throw eval_error("invalid syntax");
    }
}

// ---- slug ------------------------------------------------------------------

// Transliteration for Latin-1 and Latin Extended-A, matching the substitution
// table gosimple/slug applies before it strips. Only the ranges a slug can
// plausibly meet are covered; anything else is dropped, as it is there.
std::string_view translit(uint32_t cp) {
    switch (cp) {
    case 0xC0: case 0xC1: case 0xC2: case 0xC3: case 0xC5: return "a";
    case 0xC4: return "a";
    case 0xC6: return "ae";
    case 0xC7: return "c";
    case 0xC8: case 0xC9: case 0xCA: case 0xCB: return "e";
    case 0xCC: case 0xCD: case 0xCE: case 0xCF: return "i";
    case 0xD0: return "d";
    case 0xD1: return "n";
    case 0xD2: case 0xD3: case 0xD4: case 0xD5: case 0xD8: return "o";
    case 0xD6: return "o";
    case 0xD9: case 0xDA: case 0xDB: return "u";
    case 0xDC: return "u";
    case 0xDD: return "y";
    case 0xDE: return "th";
    case 0xDF: return "ss";
    case 0xE0: case 0xE1: case 0xE2: case 0xE3: case 0xE5: return "a";
    case 0xE4: return "a";
    case 0xE6: return "ae";
    case 0xE7: return "c";
    case 0xE8: case 0xE9: case 0xEA: case 0xEB: return "e";
    case 0xEC: case 0xED: case 0xEE: case 0xEF: return "i";
    case 0xF0: return "d";
    case 0xF1: return "n";
    case 0xF2: case 0xF3: case 0xF4: case 0xF5: case 0xF8: return "o";
    case 0xF6: return "o";
    case 0xF9: case 0xFA: case 0xFB: return "u";
    case 0xFC: return "u";
    case 0xFD: case 0xFF: return "y";
    case 0xFE: return "th";
    default: return {};
    }
}

// The per-language substitutions gosimple/slug applies to whole symbols before
// transliterating. `&` is the one that shows up in the documented examples.
std::string_view amp_word(std::string_view lang) {
    if (lang == "fr") return "et";
    if (lang == "de") return "und";
    if (lang == "es") return "y";
    if (lang == "it") return "e";
    if (lang == "pt") return "e";
    if (lang == "nl") return "en";
    if (lang == "pl") return "i";
    return "and";                                   // en and everything else
}

// ---- strip_html ------------------------------------------------------------

// bluemonday with an empty policy: every element is removed and the remaining
// text is HTML-escaped on the way out. Comments and the contents of <script>
// and <style> go with the tags.
bool tag_name_is(std::string_view tag, std::string_view name) {
    if (tag.size() < name.size()) return false;
    for (size_t i = 0; i < name.size(); ++i)
        if (std::tolower(static_cast<unsigned char>(tag[i])) != name[i]) return false;
    return tag.size() == name.size() ||
           tag[name.size()] == ' ' || tag[name.size()] == '\t' ||
           tag[name.size()] == '\n' || tag[name.size()] == '/' ||
           tag[name.size()] == '>';
}

} // namespace

// ---- HTML -------------------------------------------------------------------

value escape_html(const value& v) {
    const std::string& s = want_string(v);
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        if (const char* e = html_escape_of(c)) out += e; else out += c;
    }
    return same_stringy(v, std::move(out));
}

value unescape_html(const value& v) {
    const std::string& s = want_string(v);
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size();) {
        if (s[i] != '&') { out += s[i++]; continue; }
        const size_t semi = s.find(';', i + 1);
        // Go bounds its search; an unterminated & is literal text.
        if (semi == std::string::npos || semi - i > 32) { out += s[i++]; continue; }
        const std::string_view body(s.data() + i + 1, semi - i - 1);
        if (!body.empty() && body[0] == '#') {
            const bool hex = body.size() > 1 && (body[1] == 'x' || body[1] == 'X');
            const std::string_view digits = body.substr(hex ? 2 : 1);
            if (digits.empty()) { out += s[i++]; continue; }
            uint32_t cp = 0;
            bool ok = true;
            for (char d : digits) {
                const int n = hex ? enc::hex_nibble(d) : (d >= '0' && d <= '9' ? d - '0' : -1);
                if (n < 0) { ok = false; break; }
                cp = cp * (hex ? 16u : 10u) + static_cast<uint32_t>(n);
                if (cp > 0x10FFFF) { cp = 0xFFFD; }
            }
            if (!ok) { out += s[i++]; continue; }
            append_utf8(out, cp);
            i = semi + 1;
            continue;
        }
        const auto it = html_entities().find(body);
        if (it == html_entities().end()) { out += s[i++]; continue; }
        append_utf8(out, it->second);
        i = semi + 1;
    }
    return same_stringy(v, std::move(out));
}

// ---- URLs -------------------------------------------------------------------

value escape_url_query(const value& v) {
    return same_stringy(v, url_escape(want_string(v), url_mode::query));
}
value unescape_url_query(const value& v) {
    return same_stringy(v, url_unescape(want_string(v), /*plus_is_space=*/true));
}
value escape_url_path(const value& v) {
    return same_stringy(v, url_escape(want_string(v), url_mode::path_segment));
}
value unescape_url_path(const value& v) {
    return same_stringy(v, url_unescape(want_string(v), /*plus_is_space=*/false));
}

// ---- file paths -------------------------------------------------------------

value filepath_join(const value& v) {
    const auto& a = want_array(v);
    std::string joined;
    for (const auto& el : a) {
        const std::string& part = want_string(el);
        if (part.empty()) continue;                 // Join skips empty elements
        if (!joined.empty()) joined += '/';
        joined += part;
    }
    if (joined.empty()) return value(std::string{});
    return value(path_clean(joined));
}

value filepath_split(const value& v) {
    const std::string& s = want_string(v);
    const size_t cut = s.find_last_of('/');
    // The directory keeps its trailing separator, which is what makes
    // `dir + file` reconstruct the original.
    std::string dir  = cut == std::string::npos ? std::string{} : s.substr(0, cut + 1);
    std::string file = cut == std::string::npos ? s : s.substr(cut + 1);
    return value::array({value(std::move(dir)), value(std::move(file))});
}

// ---- quoting ----------------------------------------------------------------

value unquote(const value& v) {
    const std::string& s = want_string(v);
    if (s.size() < 2) throw eval_error("invalid syntax");
    const char q = s.front();
    if (s.back() != q) throw eval_error("invalid syntax");
    const std::string_view body(s.data() + 1, s.size() - 2);
    // A back-quoted literal is raw: no escape processing at all, and carriage
    // returns are dropped, exactly as strconv.Unquote does.
    if (q == '`') {
        std::string out;
        out.reserve(body.size());
        for (char c : body) if (c != '\r') out += c;
        return same_stringy(v, std::move(out));
    }
    if (q != '"' && q != '\'') throw eval_error("invalid syntax");
    std::string out;
    out.reserve(body.size());
    for (size_t i = 0; i < body.size();) {
        if (body[i] == '\n') throw eval_error("invalid syntax");
        if (body[i] != '\\') { out += body[i++]; continue; }
        ++i;
        unquote_escape(body, i, q, out);
    }
    // A single-quoted literal holds exactly one code point.
    if (q == '\'') {
        size_t i = 0;
        if (out.empty()) throw eval_error("invalid syntax");
        next_rune(out, i);
        if (i != out.size()) throw eval_error("invalid syntax");
    }
    return same_stringy(v, std::move(out));
}

// ---- bulk replacement --------------------------------------------------------

namespace {
std::string replace_pairs(std::string s, const std::vector<value>& items) {
    if (items.size() % 2 != 0)
        throw eval_error("invalid arg, replacements should be in pairs and must "
                         "therefore be even");
    for (size_t k = 0; k + 1 < items.size(); k += 2) {
        const std::string& from = want_string(items[k]);
        const std::string& to   = want_string(items[k + 1]);
        if (from.empty()) continue;
        std::string out;
        size_t start = 0;
        for (;;) {
            const size_t at = s.find(from, start);
            if (at == std::string::npos) { out.append(s, start, std::string::npos); break; }
            out.append(s, start, at - start);
            out += to;
            start = at + from.size();
        }
        s = std::move(out);
    }
    return s;
}
} // namespace

value replace_all_many(const value& v, const value& items) {
    return same_stringy(v, replace_pairs(want_string(v), want_array(items)));
}

// ---- slug and strip_html ------------------------------------------------------

value slug(const value& v, const value& lang_v) {
    const std::string& s = want_string(v);
    const std::string lang = lang_v.is_stringy() ? lang_v.as_string() : std::string("en");
    const std::string_view amp = amp_word(lang);

    std::string lowered;
    lowered.reserve(s.size());
    for (size_t i = 0; i < s.size();) {
        const uint32_t cp = next_rune(s, i);
        if (cp == '&') { lowered += ' '; lowered.append(amp); lowered += ' '; continue; }
        if (cp < 0x80) {
            lowered += static_cast<char>(std::tolower(static_cast<unsigned char>(cp)));
            continue;
        }
        // Uppercase Latin-1 transliterates to its lowercase ASCII form, so the
        // case fold and the transliteration are the same step.
        const std::string_view t = translit(cp);
        if (!t.empty()) lowered.append(t);
        // Anything with no transliteration is dropped, becoming a word break.
        else lowered += ' ';
    }

    std::string out;
    out.reserve(lowered.size());
    bool pending_sep = false;
    for (char c : lowered) {
        const auto u = static_cast<unsigned char>(c);
        if (std::isalnum(u)) {
            if (pending_sep && !out.empty()) out += '-';
            pending_sep = false;
            out += static_cast<char>(std::tolower(u));
        } else {
            pending_sep = true;                     // collapses runs to one dash
        }
    }
    return value(std::move(out));
}

value strip_html(const value& v, const value& preserve) {
    const std::string& s = want_string(v);
    std::vector<std::string> keep;
    if (preserve.type() == vtype::array)
        for (const auto& el : preserve.arr()) keep.push_back(want_string(el));
    else if (!preserve.is_null() && !preserve.is_nothing())
        wrong("array", preserve);

    std::string out;
    out.reserve(s.size());
    size_t i = 0;
    // Skipping a whole element (rather than just its tags) is what bluemonday
    // does for script and style: their text is code, not content.
    auto skip_element = [&](std::string_view name) {
        const std::string close = "</" + std::string(name);
        const size_t at = s.find(close, i);
        i = at == std::string::npos ? s.size() : at;
        if (i < s.size()) {
            const size_t gt = s.find('>', i);
            i = gt == std::string::npos ? s.size() : gt + 1;
        }
    };
    while (i < s.size()) {
        if (s[i] != '<') {
            if (const char* e = html_escape_of(s[i])) out += e; else out += s[i];
            ++i;
            continue;
        }
        if (s.compare(i, 4, "<!--") == 0) {
            const size_t end = s.find("-->", i + 4);
            i = end == std::string::npos ? s.size() : end + 3;
            continue;
        }
        const size_t gt = s.find('>', i);
        if (gt == std::string::npos) { out += s[i++]; continue; }   // a stray '<'
        std::string_view tag(s.data() + i + 1, gt - i - 1);
        const bool closing = !tag.empty() && tag.front() == '/';
        if (closing) tag.remove_prefix(1);
        if (!closing && (tag_name_is(tag, "script") || tag_name_is(tag, "style"))) {
            const std::string_view name = tag_name_is(tag, "script") ? "script" : "style";
            i = gt + 1;
            skip_element(name);
            continue;
        }
        bool kept = false;
        for (const auto& k : keep) {
            if (tag_name_is(tag, k)) {
                out += closing ? "</" : "<";
                out.append(k);
                out += '>';
                kept = true;
                break;
            }
        }
        (void)kept;
        i = gt + 1;
    }
    return value(std::move(out));
}

} // namespace sf::m
