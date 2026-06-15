package tenure

import (
	"context"
	"errors"
	"log"
	"strconv"
	"sync/atomic"
	"time"
)

type LeaderState struct {
	Data   any
	Leader bool
	Token  int64
}

type Callback func(ctx context.Context, st LeaderState)

type Node struct {
	store  Store
	name   string
	id     string
	lease  time.Duration
	cb     Callback
	data   any
	logger *log.Logger
	now    func() time.Time

	leader atomic.Bool
	token  atomic.Int64
}

type Option func(*Node)

func WithID(id string) Option          { return func(n *Node) { n.id = id } }
func WithLease(d time.Duration) Option { return func(n *Node) { n.lease = d } }
func WithCallback(cb Callback) Option  { return func(n *Node) { n.cb = cb } }
func WithCallbackData(d any) Option    { return func(n *Node) { n.data = d } }
func WithLogger(l *log.Logger) Option  { return func(n *Node) { n.logger = l } }

var idSeq atomic.Int64

func New(store Store, name string, opts ...Option) *Node {
	n := &Node{
		store: store,
		name:  name,
		lease: 5 * time.Second,
		now:   time.Now,
	}
	for _, o := range opts {
		o(n)
	}
	if n.id == "" {
		n.id = "node-" + strconv.FormatInt(idSeq.Add(1), 10)
	}
	if n.lease < 10*time.Millisecond {
		n.lease = 10 * time.Millisecond
	}
	return n
}

func (n *Node) IsLeader() bool { return n.leader.Load() }

func (n *Node) Token() int64 { return n.token.Load() }

func (n *Node) ID() string { return n.id }

func (n *Node) Run(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		n.runLoop(ctx)
	}()
	return done
}

func (n *Node) logf(format string, args ...any) {
	if n.logger != nil {
		n.logger.Printf(format, args...)
	}
}

func (n *Node) runLoop(ctx context.Context) {
	var (
		leader    bool
		wasLeader bool
		firstRun  = true
		token     int64
	)

	emit := func(isLeader bool, tok int64) {
		n.leader.Store(isLeader)
		if isLeader {
			n.token.Store(tok)
		} else {
			n.token.Store(0)
		}
		if n.cb == nil {
			return
		}
		if isLeader {
			n.cb(ctx, LeaderState{Data: n.data, Leader: true, Token: tok})
		} else {
			n.cb(context.Background(), LeaderState{Data: n.data, Leader: false})
		}
	}

	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			if leader {
				_ = n.store.Release(context.Background(), n.name, n.id, token)
				emit(false, 0)
			}
			return
		case <-timer.C:
		}

		var next time.Duration
		if leader {
			tok, err := n.store.Renew(ctx, n.name, n.id, token, n.lease)
			switch {
			case err == nil:
				token = tok
				n.token.Store(tok)
				next = n.lease / 2
			case errors.Is(err, ErrSuperseded):

				n.logf("[%s] superseded; stepping down", n.id)
				leader = false
				next = n.lease / 3
			default:
				n.logf("[%s] renew failed: %v", n.id, err)
				next = n.lease / 3
			}
		} else {
			tok, err := n.store.Acquire(ctx, n.name, n.id, n.lease)
			if err == nil {
				leader = true
				token = tok
				next = n.lease / 2
			} else {
				next = n.lease / 3
			}
		}

		if leader != wasLeader || firstRun {
			emit(leader, token)
			wasLeader = leader
			firstRun = false
		}

		if next <= 0 {
			next = time.Millisecond
		}
		timer.Reset(next)
	}
}
