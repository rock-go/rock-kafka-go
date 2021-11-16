package producer

import (
	"context"
	"github.com/rock-go/rock/logger"
	"golang.org/x/time/rate"
)

type Limiter struct {
	limit  *rate.Limiter
	ctx    context.Context
	cancel context.CancelFunc
}

func newLimiter(n int) *Limiter {
	ctx, cancel := context.WithCancel(context.TODO())
	if n <= 0 {
		return &Limiter{limit: nil, ctx: ctx, cancel: cancel}
	}

	return &Limiter{rate.NewLimiter(rate.Limit(n), n*2), ctx, cancel}
}

func (lt *Limiter) Handler(name string, id int) {
	if lt.limit == nil {
		return
	}

	err := lt.limit.Wait(lt.ctx)
	if err != nil {
		logger.Errorf("%s thread.idx=%d limit wait err: %v", name, id, err)
		return
	}
}

func (lt *Limiter) Close() {
	if lt.limit == nil {
		return
	}
	lt.cancel()
}
