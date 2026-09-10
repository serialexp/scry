# scry-errorsd

Runtime foundation and bounded single-writer occurrence reconciler for `scry errors`.
It opens object storage, proves conditional-write semantics, owns the deployment
manifest, and projects bounded pages of server-stamped logs schema-v3 blocks into
deterministic immutable occurrence Parquet plus metadata-last commits. Every
published object is read back,
verified, decoded with strict bounds, and folded into rebuildable `errors.sqlite`.
The role owns `catalog.sqlite` as rebuildable local state: each pass folds committed
occurrences first, then advances a bounded raw-logs metadata walk against authoritative
object storage, and only then processes a bounded page of cataloged source blocks. A
fresh, lost, or stale local catalog therefore does not hide committed schema-v3 logs.

`reconcile` performs one bounded pass and exits, failing if that pass fails. `serve`
repeats bounded passes with a completion-relative `--reconcile-interval-secs` delay;
transient pass failures are reflected in bounded status and retried until graceful
shutdown. Processing is serial (one source block at a time), and block count, source
rows, compressed bytes, decoded raw bytes, occurrence rows, and projection bytes are
all independently bounded. Clustered mode remains fail-closed until lease-backed
orchestration exists.

The workspace's `scry errors` multicall command delegates to `scry_errorsd::Args` and
`scry_errorsd::run`.
