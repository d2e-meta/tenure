# tenure

A small lease-based **leader-election** library for Go.

Many nodes compete for a named lease in a shared `Store`; whichever node holds an
unexpired lease is the leader. Leadership is **revocable** — a leader must keep
renewing its lease, and the moment it can no longer prove it holds the lease it
must stop acting as leader. Callers observe leadership through a callback that is
handed a `context.Context` cancelled the instant leadership is lost, so
leader-only work is torn down automatically instead of being gated on a racy
"am I the leader?" check.

## Install

```bash
go get github.com/d2e-meta/tenure
```

## Usage

```go
store := tenure.NewMemStore() // swap for a real backend in production

node := tenure.New(store, "reconcile-job",
    tenure.WithID("node-1"),
    tenure.WithLease(15*time.Second),
    tenure.WithCallback(func(ctx context.Context, st tenure.LeaderState) {
        if !st.Leader {
            return // lost leadership; ctx for prior work is already cancelled
        }
        // We are the leader. Bind all leader-only work to ctx; it is cancelled
        // the moment we lose the lease. Use st.Token as a fencing token for
        // conditional downstream writes (accept only if token >= last seen).
        go runLeaderWork(ctx, st.Token)
    }),
)

node.Run(ctx) // returns a channel closed when the loop stops
```

## How it works

- **Lease + renew.** The leader renews its lease roughly every half-lease. A
  follower periodically attempts to acquire; it only succeeds once the current
  lease has expired.
- **Loss detection.** Two signals end leadership: the store reporting that the
  lease was taken over (`ErrSuperseded`), or the leader being unable to renew
  before the lease would expire (e.g. during a backend outage), in which case it
  steps down so another node can safely take over.
- **Fencing token.** `LeaderState.Token` is monotonic. Because a stalled leader
  can briefly outlive its lease, the token lets downstream resources reject a
  stale writer (`WHERE token >= last_seen`). `Store.Fence` demonstrates this.

## Store backends

`Store` is the coordination backend. A production deployment must back it with a
linearizable, fault-tolerant system (Cloud Spanner, etcd, ZooKeeper, Postgres).

`MemStore` is an in-process implementation used by the tests: a mutex-guarded map
where concurrent goroutines stand in for distributed nodes. It is **not** fault
tolerant and does not work across processes — it exists to drive the election
logic in tests without external dependencies.

## Status / known limitations

- Loss detection when the backend becomes unreachable is **not yet complete**:
  on a transient renew failure the leader currently keeps assuming leadership
  rather than stepping down before its lease expires (see the `TODO`s in
  `runLoop`). Until this is addressed, rely on the fencing token for write safety.
- `MemStore` is for tests only; there is no production `Store` implementation in
  this repo yet.

## Testing

```bash
go test ./...
go test -race ./...
```

## License

Apache-2.0.
