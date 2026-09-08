# Shared Kafka test environment: making this image's JDK usable, and the JVM
# options a Kafka tool needs here.
#
# Sourced by tools/run_kafka_itest.sh and tools/run_kafka_chaos.sh. It expects
# $WORK to be set to a scratch directory the caller cleans up, and exports
# JAVA_HOME, PATH, KAFKA_JMX_OPTS and KAFKA_OPTS.
#
# Not executable on its own.

# ---- making the system JDK usable ------------------------------------------
#
# This image's JDK has been shipped broken in two different ways during this
# project, so what follows probes rather than assumes. The test is whether the
# JVM can initialise java.security AT ALL: every Kafka tool touches it before
# main(), and checking that the file merely exists is not enough -- Fedora's
# java.security `include`s /etc/crypto-policies/back-ends/java.config, and a
# missing include is a hard InternalError.
#
# Three steps, cheapest first. Each one is skipped when the previous already
# works, so on a healthy machine none of this runs.
JDK_CACHE=${SWORDFISH_JDK_CACHE:-${TMPDIR:-/tmp}/swordfish-usable-jdk}
SF_JAVA_OPTS=""

# Where the JVM thinks it lives. NOT derived from the launcher's path: on Fedora
# `java` is /usr/bin/java -> /etc/alternatives/java -> the real launcher, so
# stripping two path components lands on /usr, and treating that as the JDK
# would make the copy below try to duplicate the entire system.
jvm_home() {
  "$1" -XshowSettings:properties -version 2>&1 | sed -n 's/^ *java\.home = //p' | head -1
}

# -XshowSettings exits 0 even when the security load throws, so the verdict has
# to come from the output rather than from the status.
#
# The output is captured rather than piped into grep on purpose. Under
# `set -o pipefail`, a `grep -q` that matches exits immediately, the JVM writing
# into the pipe dies of SIGPIPE, and the pipeline's status becomes 141 -- so
# "the JVM is broken" and "the JVM is fine" both came out as failure, and the
# probe silently reported every JDK as healthy.
jvm_security_works() {
  local out
  out=$("$1" ${2:-} -XshowSettings:security -version 2>&1 || true)
  case "$out" in
    *InternalError*|*"Unable to include"*|*"Error loading java.security"*) return 1 ;;
  esac
  return 0
}

setup_java() {
  local launcher real
  if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
    launcher="$JAVA_HOME/bin/java"
  else
    launcher=$(command -v java 2>/dev/null || true)
    [ -n "$launcher" ] || launcher=/usr/lib/jvm/java-latest-openjdk/bin/java
  fi
  [ -x "$launcher" ] || { echo "no JDK found; put java on PATH or set JAVA_HOME" >&2; exit 2; }

  real=$(jvm_home "$launcher")
  [ -n "$real" ] && [ -x "$real/bin/java" ] || {
    echo "could not determine java.home from $launcher" >&2; exit 2; }
  export JAVA_HOME="$real"
  export PATH="$real/bin:$PATH"

  # 1. The JDK is whole. The normal case.
  jvm_security_works "$real/bin/java" && return

  # 2. Only the system crypto-policies include is missing. Red Hat's JDK build
  #    documents a switch for exactly this, in the comments of its own
  #    java.security, so use it rather than writing policy into /etc.
  if jvm_security_works "$real/bin/java" -Dredhat.crypto-policies=false; then
    echo "this JDK cannot read the system crypto-policies; disabling them for this run"
    SF_JAVA_OPTS="-Dredhat.crypto-policies=false"
    return
  fi

  # 3. The configuration tree is missing outright, which is how this image
  #    shipped earlier. Cache a JDK whose conf directory is real and
  #    self-contained. It has to be a copy of the WHOLE tree, not of bin/ with
  #    lib/ symlinked: the launcher derives java.home from where libjli.so was
  #    loaded from, so a symlinked lib/ leads it back to the original and its
  #    broken conf. 250MB, hence the cache.
  if [ ! -r "$JDK_CACHE/conf/security/java.security" ]; then
    echo "this JDK cannot load java.security at all; caching a usable copy in $JDK_CACHE"
    rm -rf "$JDK_CACHE"
    cp -a "$real" "$JDK_CACHE"
    rm -f "$JDK_CACHE/conf"
    mkdir -p "$JDK_CACHE/conf/security"
    # The minimum a JVM needs to reach main(): a provider list, a randomness
    # source and a keystore type. No includes, so nothing else can be missing.
    # A local PLAINTEXT broker needs nothing more.
    cat > "$JDK_CACHE/conf/security/java.security" <<'SECURITY'
security.provider.1=SUN
security.provider.2=SunRsaSign
security.provider.3=SunEC
security.provider.4=SunJSSE
security.provider.5=SunJCE
securerandom.source=file:/dev/urandom
securerandom.strongAlgorithms=NativePRNGBlocking:SUN,DRBG:SUN
securerandom.drbg.config=
keystore.type=pkcs12
keystore.type.compat=true
package.access=
package.definition=
SECURITY
  fi
  export JAVA_HOME="$JDK_CACHE"
  export PATH="$JDK_CACHE/bin:$PATH"
  jvm_security_works "$JDK_CACHE/bin/java" || {
    echo "even a self-contained JDK cannot start; see $JDK_CACHE" >&2; exit 2; }
}
setup_java


# Kafka's launcher turns on the JVM management agent by default, which needs
# $JAVA_HOME/conf/management/management.properties. A headless JDK may not ship
# one; an empty value here is not enough, it has to be non-empty to suppress it.
export KAFKA_JMX_OPTS=" "
export KAFKA_OPTS="${KAFKA_OPTS:-} $SF_JAVA_OPTS"
