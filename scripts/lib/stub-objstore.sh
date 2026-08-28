# shellcheck shell=bash
#
# stub-objstore.sh — a stand-in S3 endpoint for smoke tests that store nothing.
#
# Sourced, not executed:  . "$(dirname "$0")/lib/stub-objstore.sh"
#
# # Why this exists
#
# Several smoke tests exercise paths that never touch object storage — the
# live-tail relay, the Valkey fleet status page — and used to point the daemons
# at a deliberately unreachable endpoint (`http://127.0.0.1:1`) so they could
# run without Garage. That stopped working when `scry query` made the cold-boot
# catalog seed **fatal**: an unreachable bucket now kills the daemon before it
# binds its listener, so the test fails during startup with no useful message.
#
# Failing that way is defensible for the daemon — serving queries out of a
# catalog that silently seeded zero blocks is worse than refusing to start — so
# the fix belongs here. This stub is reachable and honestly empty: it answers
# the two requests a cold boot makes, which seeds a catalog of zero blocks and
# lets the daemon come up.
#
# It is NOT a general object store: it stores nothing, and any PUT is a 501. A
# test that writes blocks wants a real Garage (`scripts/dev-garage-up.sh`).
#
# # Usage
#
#   start_stub_objstore 127.0.0.1:14446 scry-smoke "$TMP/stub-s3.log"
#   PIDS+=("$STUB_OBJSTORE_PID")
#
# Exports SCRY_OBJSTORE_* for every daemon started afterwards, and sets
# `STUB_OBJSTORE_PID`. Returns non-zero if the stub never binds.

# Answers:
#   GET  <bucket>?list-type=2      -> 200, an empty ListBucketResult
#   GET/HEAD anything else         -> 404 (so a snapshot probe reports "absent")
#   PUT/POST/DELETE                -> 501 (writing here is a test bug, not a pass)
_STUB_OBJSTORE_PY='
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BUCKET = sys.argv[2]
EMPTY_LIST = (
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
    f"<Name>{BUCKET}</Name><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys>"
    "<IsTruncated>false</IsTruncated></ListBucketResult>"
).encode()


class H(BaseHTTPRequestHandler):
    def _send(self, status, body=b"", ctype=None):
        self.send_response(status)
        if ctype:
            self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body and self.command != "HEAD":
            self.wfile.write(body)

    def do_GET(self):
        if "list-type=2" in self.path:
            self._send(200, EMPTY_LIST, "application/xml")
        else:
            self._send(404)

    do_HEAD = do_GET

    def _refuse(self):
        # Loud on purpose: a smoke test that writes objects here is misconfigured.
        sys.stderr.write(f"stub-objstore: refusing {self.command} {self.path}\n")
        sys.stderr.flush()
        self._send(501)

    do_PUT = do_POST = do_DELETE = _refuse

    def log_message(self, *a):
        pass


ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
'

# start_stub_objstore ADDR BUCKET LOGFILE
start_stub_objstore() {
  local addr="$1" bucket="$2" logfile="$3"
  local port="${addr##*:}" host="${addr%:*}"

  python3 -c "$_STUB_OBJSTORE_PY" "$port" "$bucket" >"$logfile" 2>&1 &
  STUB_OBJSTORE_PID=$!

  local i
  for i in $(seq 1 100); do
    if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
      exec 3>&- 3<&-
      break
    fi
    sleep 0.1
  done
  if ! (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
    echo "stub-objstore: never bound $addr (see $logfile)" >&2
    return 1
  fi
  exec 3>&- 3<&-

  export SCRY_OBJSTORE_ENDPOINT="http://$addr"
  export SCRY_OBJSTORE_REGION="garage"
  export SCRY_OBJSTORE_BUCKET="$bucket"
  export SCRY_OBJSTORE_ACCESS_KEY_ID="dummy"
  export SCRY_OBJSTORE_SECRET_ACCESS_KEY="dummy"
  export SCRY_OBJSTORE_PATH_STYLE="true"
}
