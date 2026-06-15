package tenure

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", timeout, msg)
}

const testLease = 80 * time.Millisecond

func TestElectsASingleLeader(t *testing.T) {
	store := NewMemStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n := New(store, "res",
		WithID("A"),
		WithLease(testLease),
	)
	n.Run(ctx)

	waitFor(t, 2*time.Second, "node A to become leader", n.IsLeader)
	if got := n.Token(); got <= 0 {
		t.Fatalf("leader should have a positive fencing token, got %d", got)
	}
}

func TestOnlyOneLeaderAtATime(t *testing.T) {
	store := NewMemStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes := []*Node{
		New(store, "res", WithID("A"), WithLease(testLease)),
		New(store, "res", WithID("B"), WithLease(testLease)),
		New(store, "res", WithID("C"), WithLease(testLease)),
	}
	for _, n := range nodes {
		n.Run(ctx)
	}

	countLeaders := func() int {
		c := 0
		for _, n := range nodes {
			if n.IsLeader() {
				c++
			}
		}
		return c
	}

	waitFor(t, 2*time.Second, "exactly one leader to emerge", func() bool { return countLeaders() == 1 })

	for i := 0; i < 200; i++ {
		if c := countLeaders(); c > 1 {
			t.Fatalf("split-brain: %d simultaneous leaders", c)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestFailoverToNewLeaderWithHigherToken(t *testing.T) {
	store := NewMemStore()

	ctxA, cancelA := context.WithCancel(context.Background())
	a := New(store, "res", WithID("A"), WithLease(testLease))
	doneA := a.Run(ctxA)
	waitFor(t, 2*time.Second, "A to lead", a.IsLeader)
	tokA := a.Token()

	cancelA()
	<-doneA

	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	b := New(store, "res", WithID("B"), WithLease(testLease))
	b.Run(ctxB)
	waitFor(t, 2*time.Second, "B to take over", b.IsLeader)
	tokB := b.Token()

	if tokB <= tokA {
		t.Fatalf("fencing token must strictly increase across leaders: tokA=%d tokB=%d", tokA, tokB)
	}
}

func TestLeaderStepsDownAndCancelsContextWhenStoreUnavailable(t *testing.T) {
	store := NewMemStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var leaderCtx context.Context
	n := New(store, "res",
		WithID("A"),
		WithLease(testLease),
		WithCallback(func(cbCtx context.Context, st LeaderState) {
			if st.Leader {
				mu.Lock()
				leaderCtx = cbCtx
				mu.Unlock()
			}
		}),
	)
	n.Run(ctx)
	waitFor(t, 2*time.Second, "A to lead", n.IsLeader)

	store.SetUnavailable(true)

	waitFor(t, 2*time.Second, "leader to step down", func() bool { return !n.IsLeader() })

	waitFor(t, time.Second, "leader work context to be cancelled", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return leaderCtx != nil && leaderCtx.Err() != nil
	})
}

func TestFenceRejectsStaleToken(t *testing.T) {
	store := NewMemStore()
	ctx := context.Background()
	applied := 0

	if err := store.Fence(ctx, "balance", 5, func() { applied++ }); err != nil {
		t.Fatalf("token 5 should apply: %v", err)
	}
	if err := store.Fence(ctx, "balance", 6, func() { applied++ }); err != nil {
		t.Fatalf("token 6 should apply: %v", err)
	}

	if err := store.Fence(ctx, "balance", 5, func() { applied++ }); !errors.Is(err, ErrSuperseded) {
		t.Fatalf("stale token 5 must be rejected with ErrSuperseded, got %v", err)
	}
	if applied != 2 {
		t.Fatalf("only the two monotonic writes should have applied, got %d", applied)
	}
}
