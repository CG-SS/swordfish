# Port allocation shared by the tests that bind a listener.
#
# Binding port 0 and closing the socket hands back a number the kernel is then
# free to give to somebody else, and ctest runs these scripts CONCURRENTLY:
# three of them drew from the same ephemeral range, so two runs could be handed
# the same port. The loser reported "nothing listened on <port>" -- or, worse,
# connected to the other test's server and compared the wrong bytes. It showed
# up as a rare flake, twice, and only under the sanitizer trees, where
# everything is slow enough to widen the window.
#
# So: each script draws from its OWN band, and a port is confirmed free by
# binding it for real before it is handed out. The bands sit BELOW
# ip_local_port_range (32768-60999 on Linux by default), which is what stops the
# kernel handing the same number to an unrelated socket on the machine.
#
# A caller sets SF_PORT_BAND before sourcing this, and must have $WORK.
: "${SF_PORT_BAND:?set SF_PORT_BAND before sourcing ports.sh}"

free_port() {
  python3 - "$SF_PORT_BAND" "$WORK/.ports" <<'PY'
import os, random, socket, sys

base, used_file = int(sys.argv[1]), sys.argv[2]
used = set()
if os.path.exists(used_file):
    used = {int(line) for line in open(used_file) if line.strip()}

# Random rather than sequential: a re-run seconds later would otherwise pick the
# same first port, which the previous run's listener may still hold in TIME_WAIT.
for _ in range(400):
    port = base + random.randrange(0, 400)
    if port in used:
        continue
    s = socket.socket()
    try:
        s.bind(("127.0.0.1", port))
    except OSError:
        continue
    finally:
        s.close()
    with open(used_file, "a") as f:
        f.write("%d\n" % port)
    print(port)
    sys.exit(0)

sys.stderr.write("no free port in band %d-%d\n" % (base, base + 399))
sys.exit(1)
PY
}
