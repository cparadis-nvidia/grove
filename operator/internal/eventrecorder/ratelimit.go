// /*
// Copyright 2026 The Grove Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// */

package eventrecorder

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	// rateLimiterGCThreshold is the number of tracked keys above which stale per-key limiters are
	// evicted, keeping memory use bounded over the operator lifetime.
	rateLimiterGCThreshold = 1024
)

// rateLimitKey derives the rate-limiting key for an event from the object identity and the event
// reason. Objects that cannot be introspected fall back to keying by reason alone.
func rateLimitKey(object runtime.Object, reason string) string {
	if accessor, err := meta.Accessor(object); err == nil {
		return fmt.Sprintf("%s/%s/%s/%s", accessor.GetNamespace(), accessor.GetName(), accessor.GetUID(), reason)
	}
	return reason
}

// eventRateLimiter maintains a token-bucket rate limiter per key so that repeated events for the
// same key are throttled while distinct keys are limited independently.
type eventRateLimiter struct {
	mu       sync.Mutex
	limiters map[string]*limiterEntry
	limit    rate.Limit
	burst    int
	ttl      time.Duration
	now      func() time.Time
}

// limiterEntry is a per-key token-bucket limiter along with the last time it was accessed, used to
// evict entries that are no longer active.
type limiterEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

func newEventRateLimiter(interval time.Duration, burst int) *eventRateLimiter {
	return &eventRateLimiter{
		limiters: make(map[string]*limiterEntry),
		limit:    rate.Every(interval),
		burst:    burst,
		ttl:      2 * interval,
		now:      time.Now,
	}
}

func (l *eventRateLimiter) allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	l.gcLocked(now)

	entry, ok := l.limiters[key]
	if !ok {
		entry = &limiterEntry{limiter: rate.NewLimiter(l.limit, l.burst)}
		l.limiters[key] = entry
	}
	entry.lastSeen = now
	return entry.limiter.AllowN(now, 1)
}

func (l *eventRateLimiter) gcLocked(now time.Time) {
	if len(l.limiters) <= rateLimiterGCThreshold {
		return
	}
	for key, entry := range l.limiters {
		if now.Sub(entry.lastSeen) > l.ttl {
			delete(l.limiters, key)
		}
	}
}
