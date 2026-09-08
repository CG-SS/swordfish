// Hand-written recursive-descent parser for Bloblang.
//
// The precedence handling is the part worth reading: benthos resolves operators
// in four LEFT-ASSOCIATIVE passes (query/arithmetic.go, NewArithmeticExpression),
// and two of those levels do NOT match C++:
//
//   pass 1:  *  /  %  |        <- `|` (coalesce) binds as tightly as multiply
//   pass 2:  +  -
//   pass 3:  == != < <= > >=   <- all six share ONE level; in C++ they do not
//   pass 4:  && ||             <- both share ONE level; in C++ && binds tighter
//
// Parsing into an explicit tree here is what lets the emitter parenthesise
// correctly later.
#include "swordfish/blobl/parse.hh"
#include "swordfish/blobl/names.hh"

#include <cctype>
#include <cstdlib>
#include <sstream>

namespace sf::blobl {

int precedence_pass(binop op) {
    switch (op) {
    case binop::mul: case binop::div: case binop::mod: case binop::coalesce: return 1;
    case binop::add: case binop::sub:                                        return 2;
    case binop::eq:  case binop::neq: case binop::lt:
    case binop::lte: case binop::gt:  case binop::gte:                       return 3;
    case binop::and_: case binop::or_:                                       return 4;
    }
    return 4;
}


std::string parse_error::render(std::string_view src) const {
    std::ostringstream os;
    os << "line " << where.line << ", col " << where.col << ": " << what() << "\n";
    uint32_t line = 1;
    size_t start = 0;
    for (size_t i = 0; i < src.size() && line < where.line; ++i)
        if (src[i] == '\n') { ++line; start = i + 1; }
    size_t stop = src.find('\n', start);
    if (stop == std::string_view::npos) stop = src.size();
    os << "  " << src.substr(start, stop - start) << "\n  "
       << std::string(where.col > 0 ? where.col - 1 : 0, ' ') << "^";
    return os.str();
}

namespace {

class parser {
public:
    explicit parser(std::string_view s) : src_(s) {}

    // One expression, assigned to root.
    mapping parse_query_only() {
        mapping m;
        skip_ws_and_comments();
        statement st;
        st.target.kind = target_kind::root;
        size_t start = i_;
        st.expr = expr();
        st.source_text = trim(std::string(src_.substr(start, i_ - start)));
        m.statements.push_back(std::move(st));
        skip_ws_and_comments();
        if (!eof()) fail("unexpected trailing content after expression");
        return m;
    }

    mapping parse_all() {
        mapping m;
        skip_ws_and_comments();
        while (!eof()) {
            // `map <name> { ... }` declares a reusable mapping rather than
            // assigning anything, so it is recognised before the statement
            // targets. Nothing else may start with `map ` -- a field called
            // `map` would be written `root.map = ...`.
            if (peek_lit("map ") || peek_lit("map\t")) {
                bump(3);
                skip_ws_and_comments();
                named_map nm;
                nm.name = ident();
                skip_ws_and_comments();
                if (cur() != '{') fail("expected '{' after a map name");
                bump();
                nm.body = std::make_shared<mapping>(parse_block());
                if (cur() != '}') fail("expected '}' to close the map definition");
                bump();
                if (m.find_map(nm.name)) fail("map '" + nm.name + "' is declared twice");
                m.maps.push_back(std::move(nm));
                skip_ws_and_comments();
                continue;
            }
            m.statements.push_back(statement_());
            skip_ws_and_comments();
            // Statements are separated by a LINE BREAK, not merely by
            // whitespace. Accepting `root.a = 1 root.b = 2` on one line was a
            // laxity rather than a wrong answer -- the reference rejects it
            // with "expected line break" -- but a mapping that parses here and
            // fails there is still a compatibility break, in the direction that
            // bites someone moving a config the other way.
            if (!eof() && !crossed_newline_) fail("expected a line break between statements");
        }
        if (m.statements.empty() && m.maps.empty()) fail("empty mapping");
        if (m.statements.empty()) fail("a mapping needs at least one assignment");
        return m;
    }

    // The statements between the braces of a map definition. Leaves the cursor
    // on the closing brace.
    mapping parse_block() {
        mapping inner;
        skip_ws_and_comments();
        while (!eof() && cur() != '}') {
            inner.statements.push_back(statement_());
            skip_ws_and_comments();
            if (!eof() && cur() != '}' && !crossed_newline_)
                fail("expected a line break between statements");
        }
        if (inner.statements.empty()) fail("a map definition needs at least one statement");
        return inner;
    }

private:
    // ---- cursor ----
    std::string_view src_;
    size_t   i_    = 0;
    uint32_t line_ = 1, col_ = 1;

    bool eof() const { return i_ >= src_.size(); }
    char cur() const { return i_ < src_.size() ? src_[i_] : '\0'; }
    char at(size_t o) const { return i_ + o < src_.size() ? src_[i_ + o] : '\0'; }
    source_span here() const { return {line_, col_}; }

    void bump() {
        if (eof()) return;
        if (src_[i_] == '\n') { ++line_; col_ = 1; } else ++col_;
        ++i_;
    }
    void bump(size_t n) { while (n--) bump(); }

    [[noreturn]] void fail(const std::string& msg) { throw parse_error(here(), msg); }

    // Mappings come from configuration files, so a deeply nested expression is
    // untrusted input. Without a limit, recursive descent overflows the stack --
    // a crash rather than a diagnostic.
    static constexpr int max_depth = 256;
    int _depth = 0;

    // Lambda parameters currently in scope, innermost last. A bare identifier
    // resolves against this; anything else is an unknown identifier.
    std::vector<std::string> _lambda_params;
    bool in_scope(const std::string& n) const {
        for (auto it = _lambda_params.rbegin(); it != _lambda_params.rend(); ++it)
            if (*it == n) return true;
        return false;
    }
    struct depth_guard {
        parser& p;
        explicit depth_guard(parser& pp) : p(pp) {
            if (++p._depth > max_depth) {
                --p._depth;
                p.fail("expression nested too deeply (limit " +
                       std::to_string(max_depth) + ")");
            }
        }
        ~depth_guard() { --p._depth; }
    };

    void skip_spaces() { while (!eof() && (cur() == ' ' || cur() == '\t')) bump(); }

    // Set while skipping if a newline went past. Statements must be separated
    // by one, so the caller needs to know.
    bool crossed_newline_ = false;

    void skip_ws_and_comments() {
        crossed_newline_ = false;
        for (;;) {
            while (!eof() && std::isspace(static_cast<unsigned char>(cur()))) {
                if (cur() == '\n') crossed_newline_ = true;
                bump();
            }
            if (cur() == '#') {
                while (!eof() && cur() != '\n') bump();
                continue;                       // the newline is consumed above
            }
            return;
        }
    }

    bool lit(std::string_view s) {
        if (src_.compare(i_, s.size(), s) != 0) return false;
        bump(s.size());
        return true;
    }

    bool peek_lit(std::string_view s) const { return src_.compare(i_, s.size(), s) == 0; }

    std::string ident() {
        if (!(std::isalpha(static_cast<unsigned char>(cur())) || cur() == '_')) fail("expected identifier");
        std::string s;
        while (std::isalnum(static_cast<unsigned char>(cur())) || cur() == '_') { s += cur(); bump(); }
        return s;
    }

    // ---- statements ----
    statement statement_() {
        size_t stmt_start = i_;
        statement st;
        if (lit("let ")) {
            skip_spaces();
            st.target.kind = target_kind::var;
            st.target.name = ident();
        // Two spellings: `meta key = ...` / `meta = ...` (space) and `meta=...`
        // (no space). The condition used to list the first twice.
        } else if (peek_lit("meta ") || peek_lit("meta=")) {
            bump(4);
            skip_spaces();
            st.target.kind = target_kind::meta;
            if (cur() == '"') st.target.name = string_lit_raw();
            else if (cur() != '=') st.target.name = ident();
        // `root` must be the WHOLE word. The `let ` and `meta ` branches above
        // each require a terminator and this one did not, so every bare-path
        // target whose field name merely STARTS with root -- `root_id`,
        // `roots`, `rooted` -- was consumed as `root` and then failed with
        // "expected '=' after assignment target", pointing at a '=' that is
        // right there. The reference renders all three normally, and the
        // bare-path branch directly below exists to handle exactly them.
        } else if (peek_lit("root") &&
                   !(std::isalnum(static_cast<unsigned char>(at(4))) || at(4) == '_')) {
            bump(4);
            st.target.kind = target_kind::root;
            while (cur() == '.') {
                bump();
                st.target.path.push_back(ident());
                st.target.kind = target_kind::root_path;
            }
        } else if (std::isalpha(static_cast<unsigned char>(cur())) || cur() == '_') {
            // A bare path assigns to a field of root: `foo = 1` means
            // `root.foo = 1`, and `foo.bar = 1` means `root.foo.bar = 1`.
            // Verified against redpanda-connect 4.107.2, which renders
            // `foo = "bar"` as {"foo":"bar"}. Four files in the Benthos test
            // corpus use this spelling and were rejected outright before.
            st.target.kind = target_kind::root_path;
            st.target.path.push_back(ident());
            while (cur() == '.') {
                bump();
                st.target.path.push_back(ident());
            }
        } else {
            fail("expected a statement target: root, meta, let or a field path");
        }

        skip_spaces();
        if (cur() != '=') fail("expected '=' after assignment target");
        bump();
        skip_ws_and_comments();

        st.expr = expr();
        st.source_text = trim(std::string(src_.substr(stmt_start, i_ - stmt_start)));
        return st;
    }

    static std::string trim(std::string s) {
        while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) s.pop_back();
        return s;
    }

    // ---- expressions ----
    //
    // Collect a flat run of operands and operators, then fold it in the same
    // four passes benthos uses. Doing it this way (rather than the usual
    // precedence-climbing recursion) keeps the correspondence to the reference
    // implementation obvious, which is what protects the semantics.
    node_ptr expr() {
        depth_guard g_(*this);
        std::vector<node_ptr> operands;
        std::vector<binop>    ops;
        operands.push_back(unary());
        for (;;) {
            size_t save_i = i_; uint32_t save_l = line_, save_c = col_;
            skip_spaces();
            auto op = try_binop();
            if (!op) { i_ = save_i; line_ = save_l; col_ = save_c; break; }
            skip_ws_and_comments();
            ops.push_back(*op);
            operands.push_back(unary());
        }
        return fold(std::move(operands), std::move(ops));
    }

    static node_ptr fold(std::vector<node_ptr> fns, std::vector<binop> ops) {
        for (int pass = 1; pass <= 4; ++pass) {
            std::vector<node_ptr> out;
            std::vector<binop>    rest;
            out.push_back(std::move(fns[0]));
            for (size_t k = 0; k < ops.size(); ++k) {
                if (precedence_pass(ops[k]) == pass) {
                    auto sp = out.back()->span;
                    out.back() = mk<binary_node>(sp, ops[k], std::move(out.back()), std::move(fns[k + 1]));
                } else {
                    out.push_back(std::move(fns[k + 1]));
                    rest.push_back(ops[k]);
                }
            }
            fns = std::move(out);
            ops = std::move(rest);
            if (fns.size() == 1) break;
        }
        return std::move(fns[0]);
    }

    std::optional<binop> try_binop() {
        // Longest match first, so "==" is not read as "=".
        if (lit("==")) return binop::eq;
        if (lit("!=")) return binop::neq;
        if (lit("<=")) return binop::lte;
        if (lit(">=")) return binop::gte;
        if (lit("&&")) return binop::and_;
        if (lit("||")) return binop::or_;
        if (cur() == '<') { bump(); return binop::lt; }
        if (cur() == '>') { bump(); return binop::gt; }
        if (cur() == '+') { bump(); return binop::add; }
        if (cur() == '-') { bump(); return binop::sub; }
        if (cur() == '*') { bump(); return binop::mul; }
        if (cur() == '/') { bump(); return binop::div; }
        if (cur() == '%') { bump(); return binop::mod; }
        if (cur() == '|') { bump(); return binop::coalesce; }
        return std::nullopt;
    }

    // Whether a `.` at the cursor starts a field or method access, looking
    // past any whitespace and newlines. A `.` that leads to anything else --
    // end of input, an operator -- is not a continuation and is left alone.
    bool dot_continues() const {
        size_t k = i_ + 1;
        while (k < src_.size() && (src_[k] == ' ' || src_[k] == '\t' ||
                                   src_[k] == '\n' || src_[k] == '\r')) ++k;
        return k < src_.size() &&
               (std::isalpha(static_cast<unsigned char>(src_[k])) || src_[k] == '_' ||
                src_[k] == '"' ||
                // A DIGIT too: `.0` is a path segment, an array index or an
                // object key named "0" depending on what it is applied to.
                std::isdigit(static_cast<unsigned char>(src_[k])));
    }
    void skip_spaces_and_newlines() {
        while (cur() == ' ' || cur() == '\t' || cur() == '\n' || cur() == '\r') bump();
    }

    node_ptr unary() {
        // Guarded like expr() and primary(). It recurses once per leading '-'
        // or '!' and had no bound at all, so 200,000 of them segfaulted every
        // entry point -- `swordfish run`, `swordfish build`, and `sfconfig
        // lint`, which is precisely the tool you would point at a config you do
        // not trust. The reference exits 1 on the same input.
        depth_guard g_(*this);
        auto sp = here();
        if (cur() == '-' && !std::isspace(static_cast<unsigned char>(at(1)))) {
            bump();
            return mk<neg_node>(sp, unary());
        }
        // `!x` is logical NOT. `!=` is a comparison and is handled by the
        // operator parser, so a `!` followed by `=` is not a prefix here.
        if (cur() == '!' && at(1) != '=') {
            bump();
            return mk<not_node>(sp, unary());
        }
        return postfix(primary());
    }

    // Method calls and field access bind tighter than any operator.
    //
    // This loop is ITERATIVE, so it does not overflow the stack itself -- but
    // every consumer of the spine it builds recurses once per link: names.cc's
    // walk(), the interpreter's eval(), the emitter's expr(), and the AST's own
    // unique_ptr destructor chain. `this` followed by 300,000 `.f`s therefore
    // segfaulted after parsing cleanly. Bounding the SPINE bounds all of them,
    // and it is the same limit and the same message as nesting, because to a
    // config author they are the same mistake.
    node_ptr postfix(node_ptr base) {
        int links = 0;
        for (;;) {
            if (++links > max_depth)
                fail("expression nested too deeply (limit " +
                     std::to_string(max_depth) + ")");
            // A quoted segment names a field whose key is not an identifier:
            // `this.a."b.c"` reads the key "b.c", not a nested path. Without
            // this the dot parsed as the end of the expression and the rest of
            // the line looked like a second statement.
            if (cur() == '.' && at(1) == '"') {
                auto sp = here();
                bump();
                std::string name = string_lit_raw();
                base = mk<field_node>(sp, std::move(base), std::move(name));
                continue;
            }
            // A `.` may be followed by whitespace or a NEWLINE before the
            // method name: chaining across lines with a trailing dot is how the
            // Benthos corpus writes long chains --
            //     this.locations.
            //         filter(...).
            //         map_each(...)
            // Requiring the name immediately after the dot rejected those files
            // with "expected a line break between statements", which points at
            // the wrong thing entirely.
            if (cur() == '.' && dot_continues()) {
                auto sp = here();
                bump();
                skip_spaces_and_newlines();
                std::string name;
                if (std::isdigit(static_cast<unsigned char>(cur()))) {
                    while (std::isdigit(static_cast<unsigned char>(cur()))) { name += cur(); bump(); }
                } else {
                    name = ident();
                }
                if (cur() == '(') {
                    auto a = arg_list();
                    base = mk<method_node>(sp, std::move(base), std::move(name),
                                           std::move(a.args), std::move(a.names));
                } else {
                    base = mk<field_node>(sp, std::move(base), std::move(name));
                }
                continue;
            }
            if (cur() == '[') {
                auto sp = here();
                bump();
                skip_ws_and_comments();
                auto idx = expr();
                skip_ws_and_comments();
                if (cur() != ']') fail("expected ']'");
                bump();
                base = mk<index_node>(sp, std::move(base), std::move(idx));
                continue;
            }
            return base;
        }
    }

    struct arg_list_result {
        std::vector<node_ptr>    args;
        std::vector<std::string> names;   // parallel; empty entry = positional
    };

    // Looks ahead for `identifier :` without consuming, so a named argument is
    // distinguishable from an expression that merely starts with an identifier.
    bool at_named_arg() const {
        size_t k = i_;
        if (!(std::isalpha(static_cast<unsigned char>(at(0))) || at(0) == '_')) return false;
        size_t off = 0;
        while (k + off < src_.size() &&
               (std::isalnum(static_cast<unsigned char>(src_[k + off])) || src_[k + off] == '_'))
            ++off;
        size_t j = k + off;
        while (j < src_.size() && (src_[j] == ' ' || src_[j] == '\t')) ++j;
        // A single ':' (not '::') separates a name from its value.
        return j < src_.size() && src_[j] == ':' && (j + 1 >= src_.size() || src_[j + 1] != ':');
    }

    arg_list_result arg_list() {
        if (cur() != '(') fail("expected '('");
        bump();
        arg_list_result out;
        skip_ws_and_comments();
        if (cur() == ')') { bump(); return out; }
        for (;;) {
            skip_ws_and_comments();
            std::string name;
            if (at_named_arg()) {
                name = ident();
                skip_spaces();
                if (cur() != ':') fail("expected ':' after argument name");
                bump();
                skip_ws_and_comments();
            }
            out.args.push_back(at_lambda() ? lambda_expr() : expr());
            out.names.push_back(std::move(name));
            skip_ws_and_comments();
            if (cur() == ',') { bump(); skip_ws_and_comments(); continue; }
            if (cur() == ')') { bump(); return out; }
            fail("expected ',' or ')' in argument list");
        }
    }

    // `ident ->` with nothing consumed yet.
    bool at_lambda() const {
        size_t k = i_;
        if (!(std::isalpha(static_cast<unsigned char>(at(0))) || at(0) == '_')) return false;
        size_t off = 0;
        while (k + off < src_.size() &&
               (std::isalnum(static_cast<unsigned char>(src_[k + off])) || src_[k + off] == '_'))
            ++off;
        size_t j = k + off;
        while (j < src_.size() && (src_[j] == ' ' || src_[j] == '\t')) ++j;
        return j + 1 < src_.size() && src_[j] == '-' && src_[j + 1] == '>';
    }

    node_ptr lambda_expr() {
        auto sp = here();
        std::string param = ident();
        skip_spaces();
        if (!lit("->")) fail("expected '->' in lambda");
        skip_ws_and_comments();
        _lambda_params.push_back(param);
        node_ptr body = expr();
        _lambda_params.pop_back();
        return mk<lambda_node>(sp, std::move(param), std::move(body));
    }

    std::string string_lit_raw() {
        // Triple-quoted raw strings: no escape processing, may span lines. Used
        // throughout the reference's own examples for regexes, JSON schemas and
        // PEM keys, where backslash escaping would be unreadable.
        if (peek_lit(R"(""")")) {
            bump(3);
            const size_t start = i_;
            while (!eof() && !peek_lit(R"(""")")) bump();
            if (eof()) fail("unterminated triple-quoted string");
            std::string out(src_.substr(start, i_ - start));
            bump(3);
            return out;
        }
        char quote = cur();
        bump();
        std::string s;
        while (!eof() && cur() != quote) {
            if (cur() == '\\') {
                bump();
                switch (cur()) {
                case 'n': s += '\n'; break; case 't': s += '\t'; break;
                case 'r': s += '\r'; break; case '\\': s += '\\'; break;
                case '"': s += '"';  break; case '\'': s += '\''; break;
                case '/': s += '/';  break; case '0': s += '\0'; break;
                default:
                    // Go's strconv.Unquote rejects unknown escapes; silently
                    // dropping the backslash would turn "\\w" in a regex into "w".
                    fail(std::string("unknown escape sequence '\\") + cur() + "'");
                }
                bump();
            } else { s += cur(); bump(); }
        }
        if (eof()) fail("unterminated string literal");
        bump();
        return s;
    }

    // An object literal's key. A bare word or a quoted string followed by ':'
    // is a literal key; anything else is an expression, which is what makes
    // `{loc.state: ...}` work. The bareword case has to be tried first, because
    // `foo` on its own is not a valid expression -- it would be read as an
    // unknown identifier.
    node_ptr object_key() {
        const auto sp = here();
        const size_t save_i = i_;
        const uint32_t save_l = line_, save_c = col_;
        auto restore = [&] { i_ = save_i; line_ = save_l; col_ = save_c; };
        if (cur() == '"' || cur() == '\'') {
            string_lit_raw();
            skip_ws_and_comments();
            const bool is_key = cur() == ':';
            restore();
            if (is_key) return mk<lit_node>(sp, value(string_lit_raw()));
        } else if (std::isalpha(static_cast<unsigned char>(cur())) || cur() == '_') {
            ident();
            skip_ws_and_comments();
            const bool is_key = cur() == ':';
            restore();
            if (is_key) return mk<lit_node>(sp, value(ident()));
        }
        return expr();
    }

    node_ptr primary() {
        depth_guard g_(*this);
        skip_spaces();
        const auto sp = here();

        if (cur() == '(') {
            bump(); skip_ws_and_comments();
            auto e = expr();
            skip_ws_and_comments();
            if (cur() != ')') fail("expected ')'");
            bump();
            return e;
        }
        if (cur() == '"' || cur() == '\'') return mk<lit_node>(sp, value(string_lit_raw()));
        if (cur() == '$') { bump(); return mk<var_node>(sp, ident()); }

        // `@` is the metadata shorthand, and it was not implemented at all --
        // `@foo`, `@"X-Foo"` and a bare `@` each failed with `unexpected
        // character`, which names a character rather than the missing form, so
        // a config copied from the reference's own documentation failed to start
        // with a misleading diagnosis.
        //
        // The grammar is benthos's metadataReferencePattern: '@' followed by an
        // OPTIONAL name, which is either a run of [A-Za-z0-9_] or a quoted
        // string. Nothing else -- `@."Content-Type"` is a bare `@` (the whole
        // metadata object) followed by ordinary path access, which the postfix
        // spine below already handles.
        //
        // It becomes a `meta` call rather than a node of its own, because
        // NewMetaFunction(key) is exactly `meta(key)` and NewMetaFunction("")
        // exactly `meta()`. That way the interpreter, the emitter and every
        // consumer of the AST get it without a line of new code, and the two
        // backends cannot disagree about it.
        if (cur() == '@') {
            bump();
            std::string key;
            if (cur() == '"' || cur() == '\'') {
                key = string_lit_raw();
            } else {
                while (std::isalnum(static_cast<unsigned char>(cur())) || cur() == '_') {
                    key += cur();
                    bump();
                }
            }
            std::vector<node_ptr> args;
            std::vector<std::string> names;
            if (!key.empty()) {
                args.push_back(mk<lit_node>(sp, value(key)));
                names.emplace_back();
            }
            return mk<call_node>(sp, std::string("meta"), std::move(args), std::move(names));
        }

        if (cur() == '[') {
            bump(); skip_ws_and_comments();
            std::vector<node_ptr> items;
            if (cur() == ']') { bump(); return mk<array_node>(sp, std::move(items)); }
            for (;;) {
                items.push_back(expr());
                skip_ws_and_comments();
                // A trailing comma is allowed, as it is in the reference: the
                // documented replace_all_many example ends its array with one.
                if (cur() == ',') {
                    bump(); skip_ws_and_comments();
                    if (cur() == ']') { bump(); break; }
                    continue;
                }
                if (cur() == ']') { bump(); break; }
                fail("expected ',' or ']'");
            }
            return mk<array_node>(sp, std::move(items));
        }

        if (cur() == '{') {
            bump(); skip_ws_and_comments();
            std::vector<object_entry> entries;
            if (cur() == '}') { bump(); return mk<object_node>(sp, std::move(entries)); }
            for (;;) {
                skip_ws_and_comments();
                entries.push_back({object_key(), nullptr});
                skip_ws_and_comments();
                if (cur() != ':') fail("expected ':' in object literal");
                bump(); skip_ws_and_comments();
                entries.back().value = expr();
                skip_ws_and_comments();
                if (cur() == ',') {
                    bump(); skip_ws_and_comments();
                    if (cur() == '}') { bump(); break; }
                    continue;
                }
                if (cur() == '}') { bump(); break; }
                fail("expected ',' or '}'");
            }
            return mk<object_node>(sp, std::move(entries));
        }

        if (std::isdigit(static_cast<unsigned char>(cur()))) {
            // Digits, and a '.' with digits after it. NO EXPONENT -- Bloblang
            // has no exponent form, and this scanner used to half-accept one.
            // It took `e`/`E` but not the sign, so `1.5e-3` accumulated "1.5e",
            // strtod took the longest valid prefix (1.5), and the parser then
            // read the '-' as subtraction: the mapping evaluated to -1.5, with
            // nothing logged and a clean exit. `1.5E+3` gave 4.5 and `2e-2`
            // gave 0. Three of four literals silently wrong is the
            // accept-and-approximate rule, in the parser.
            //
            // Accepting the exponent properly would be the other obvious fix and
            // is wrong: the reference has no such literal either. Its Number
            // combinator (benthos internal/bloblang/parser/combinators.go) reads
            // an optional minus, digits, and optionally '.' plus digits -- that
            // is the whole grammar -- and `redpanda-connect` refuses every one
            // of `1e3`, `1.5e-3`, `1.5E+3` and `2e-2` with a lint error. So the
            // `e` is left for the parser to trip over, which turns a wrong
            // number into a named refusal.
            std::string t;
            bool is_float = false;
            while (std::isdigit(static_cast<unsigned char>(cur())) || cur() == '.') {
                // `10.pow(-2)` is a method call on 10, not the float `10.`
                // followed by junk: a '.' only continues the number when a
                // digit follows it.
                if (cur() == '.' && !std::isdigit(static_cast<unsigned char>(at(1)))) break;
                if (cur() == '.') is_float = true;
                t += cur(); bump();
            }
            return is_float ? mk<lit_node>(sp, value(std::strtod(t.c_str(), nullptr)))
                            : mk<lit_node>(sp, value(static_cast<int64_t>(std::strtoll(t.c_str(), nullptr, 10))));
        }

        if (std::isalpha(static_cast<unsigned char>(cur())) || cur() == '_') {
            std::string name = ident();
            if (in_scope(name)) return mk<var_node>(sp, std::move(name));
            if (name == "this") return mk<this_node>(sp);
            if (name == "true")  return mk<lit_node>(sp, value(true));
            if (name == "false") return mk<lit_node>(sp, value(false));
            if (name == "null")  return mk<lit_node>(sp, value());
            if (name == "if") {
                skip_ws_and_comments();
                auto cond = expr();
                skip_ws_and_comments();
                if (cur() != '{') fail("expected '{' after if condition");
                bump(); skip_ws_and_comments();
                auto then_ = expr();
                skip_ws_and_comments();
                if (cur() != '}') fail("expected '}' after if body");
                bump(); skip_ws_and_comments();
                node_ptr else_;
                if (lit("else")) {
                    skip_ws_and_comments();
                    if (peek_lit("if")) {
                        else_ = primary();           // else-if chain
                    } else {
                        if (cur() != '{') fail("expected '{' after else");
                        bump(); skip_ws_and_comments();
                        else_ = expr();
                        skip_ws_and_comments();
                        if (cur() != '}') fail("expected '}' after else body");
                        bump();
                    }
                }
                return mk<if_node>(sp, std::move(cond), std::move(then_), std::move(else_));
            }
            if (name == "match") {
                skip_ws_and_comments();
                node_ptr ctx_expr;
                // An optional context expression sits between `match` and the
                // brace: `match this.foo { ... }`.
                if (cur() != '{') {
                    ctx_expr = expr();
                    skip_ws_and_comments();
                }
                if (cur() != '{') fail("expected '{' after match");
                bump();
                std::vector<std::pair<node_ptr, node_ptr>> cases;
                for (;;) {
                    skip_ws_and_comments();
                    if (cur() == '}') { bump(); break; }
                    if (eof()) fail("unterminated match expression");
                    node_ptr cond;
                    // `_` is the catch-all. Recognised before the expression
                    // parser sees it, which would read it as an identifier.
                    if (cur() == '_' && !std::isalnum(static_cast<unsigned char>(at(1))) &&
                        at(1) != '_') {
                        bump();
                    } else {
                        cond = expr();
                    }
                    skip_ws_and_comments();
                    if (!lit("=>")) fail("expected '=>' in a match case");
                    skip_ws_and_comments();
                    auto val = expr();
                    cases.emplace_back(std::move(cond), std::move(val));
                    skip_ws_and_comments();
                    if (cur() == ',') { bump(); }
                }
                if (cases.empty()) fail("a match expression needs at least one case");
                return mk<match_node>(sp, std::move(ctx_expr), std::move(cases));
            }
            // Bare identifier followed by '(' is a function call.
            if (cur() == '(') {
                auto a = arg_list();
                return mk<call_node>(sp, std::move(name), std::move(a.args), std::move(a.names));
            }
            // A bare identifier with NO parentheses is a field of `this`:
            // `root = foo` means `root = this.foo`, and inside `map_each` it
            // reads the element's field. Verified against redpanda-connect
            // 4.107.2; six files in the Benthos test corpus rely on it.
            //
            // This does not weaken the rule that an unimplemented method must
            // be a NAMED error rather than a silent approximation: that rule is
            // about calls, and `foo(...)` with parentheses still fails above
            // when the name is unknown. Only the parenthesis-free spelling,
            // which cannot be a call, becomes a field read.
            return mk<field_node>(sp, mk<this_node>(sp), std::move(name));
        }

        fail("unexpected character");
    }
};

} // namespace

namespace {
// Names are resolved here, at build time, exactly as Benthos does. Deferring to
// eval would let `|` and `.catch()` swallow an unimplemented method and turn it
// into a wrong answer.
mapping validated(mapping m) {
    auto errs = validate_names(m);
    if (!errs.empty()) throw parse_error(errs.front().where, errs.front().message);
    return m;
}
}

mapping parse_mapping(std::string_view src) { return validated(parser(src).parse_all()); }
mapping parse_query(std::string_view src)   { return validated(parser(src).parse_query_only()); }

} // namespace sf::blobl

namespace sf::blobl {

namespace {

// The dotted path an expression reads, or empty when it is not a path at all.
// `this.a.0` comes back whole; `this.a.foo()` does not, because a method result
// has no path to name.
std::string describe_path(const node& n) {
    return std::visit([&](const auto& k) -> std::string {
        using T = std::decay_t<decltype(k)>;
        if constexpr (std::is_same_v<T, this_node>) {
            return "this";
        }
        else if constexpr (std::is_same_v<T, field_node>) {
            const std::string base = describe_path(*k.target);
            return base.empty() ? std::string() : base + "." + k.key;
        }
        else if constexpr (std::is_same_v<T, var_node>) {
            return "$" + k.name;
        }
        else {
            return {};
        }
    }, n.kind);
}

} // namespace

source_desc describe_source(const node& n) {
    // A field path is rebuilt by walking back to its root, so the message names
    // the whole path (`this.a.0`) rather than just the last segment.
    return std::visit([&](const auto& k) -> source_desc {
        using T = std::decay_t<decltype(k)>;
        if constexpr (std::is_same_v<T, field_node>) {
            const std::string base = describe_path(*k.target);
            return base.empty() ? source_desc{} : source_desc{"field `" + base + "." + k.key + "`", false};
        }
        else if constexpr (std::is_same_v<T, this_node>) {
            return source_desc{"field `this`", false};
        }
        else if constexpr (std::is_same_v<T, method_node>) {
            return source_desc{"method " + k.name, false};
        }
        else if constexpr (std::is_same_v<T, call_node>) {
            return source_desc{"function " + k.name, false};
        }
        else if constexpr (std::is_same_v<T, lit_node>) {
            // The reference puts the literal's value inside the description
            // rather than appending it, so this is spelled out in full here.
            if (k.v.type() == vtype::string)
                return source_desc{"string literal (" + k.v.to_json() + ")", true};
            return {};
        }
        else {
            return {};
        }
    }, n.kind);
}

} // namespace sf::blobl
