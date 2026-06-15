package tenure

import (
	"context"
	"sync"
	"time"
)

type memRow struct {
	owner  string
	token  int64
	expiry time.Time
}

type MemStore struct {
	mu          sync.Mutex
	now         func() time.Time
	counter     int64
	rows        map[string]*memRow
	seen        map[string]int64
	unavailable bool
}

func NewMemStore() *MemStore {
	return &MemStore{
		now:  time.Now,
		rows: make(map[string]*memRow),
		seen: make(map[string]int64),
	}
}

func (m *MemStore) SetUnavailable(v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unavailable = v
}

func (m *MemStore) Acquire(ctx context.Context, name, owner string, lease time.Duration) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.unavailable {
		return 0, ErrUnavailable
	}
	now := m.now()
	if r, ok := m.rows[name]; ok && r.owner != owner && now.Before(r.expiry) {
		return 0, ErrHeld
	}
	m.counter++
	m.rows[name] = &memRow{owner: owner, token: m.counter, expiry: now.Add(lease)}
	return m.counter, nil
}

func (m *MemStore) Renew(ctx context.Context, name, owner string, oldToken int64, lease time.Duration) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.unavailable {
		return 0, ErrUnavailable
	}
	r, ok := m.rows[name]
	if !ok || r.owner != owner || r.token != oldToken {
		return 0, ErrSuperseded
	}
	m.counter++
	r.token = m.counter
	r.expiry = m.now().Add(lease)
	return r.token, nil
}

func (m *MemStore) Release(ctx context.Context, name, owner string, token int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.rows[name]; ok && r.owner == owner && r.token == token {
		delete(m.rows, name)
	}
	return nil
}

func (m *MemStore) Fence(ctx context.Context, name string, token int64, apply func()) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if token < m.seen[name] {
		return ErrSuperseded
	}
	m.seen[name] = token
	apply()
	return nil
}
