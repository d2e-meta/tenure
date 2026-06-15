package tenure

import (
	"context"
	"errors"
	"time"
)

var (
	ErrHeld = errors.New("tenure: lock held by another owner")

	ErrSuperseded = errors.New("tenure: token superseded")

	ErrUnavailable = errors.New("tenure: store unavailable")
)

type Store interface {
	Acquire(ctx context.Context, name, owner string, lease time.Duration) (token int64, err error)

	Renew(ctx context.Context, name, owner string, oldToken int64, lease time.Duration) (token int64, err error)

	Release(ctx context.Context, name, owner string, token int64) error

	Fence(ctx context.Context, name string, token int64, apply func()) error
}
