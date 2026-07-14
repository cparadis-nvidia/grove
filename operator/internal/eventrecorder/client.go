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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	ctrl "sigs.k8s.io/controller-runtime"
)

// Client wraps a Kubernetes event recorder and applies configured per-type rate limits before
// submitting events.
type Client struct {
	delegate record.EventRecorder
	config   Config
	limiters map[int]*eventRateLimiter
}

var _ record.EventRecorder = (*Client)(nil)

// New returns a rate-limiting event submitter that implements record.EventRecorder.
func New(delegate record.EventRecorder, config Config) *Client {
	limiters := make(map[int]*eventRateLimiter, len(config.Rules))
	for i, rule := range config.Rules {
		limiters[i] = newEventRateLimiter(rule.Interval, rule.Burst)
	}
	return &Client{
		delegate: delegate,
		config:   config,
		limiters: limiters,
	}
}

// ForManager builds a Client for the named controller using DefaultConfig().
func ForManager(mgr ctrl.Manager, name string) *Client {
	return New(mgr.GetEventRecorderFor(name), DefaultConfig())
}

// NewTest returns a Client with no rate-limit rules for unit tests.
func NewTest(delegate record.EventRecorder) *Client {
	return New(delegate, Config{})
}

// Event records an event, applying configured rate limits when the event type and reason match.
func (c *Client) Event(object runtime.Object, eventType, reason, message string) {
	if !c.allow(object, eventType, reason) {
		return
	}
	c.delegate.Event(object, eventType, reason, message)
}

// Eventf records a formatted event, applying configured rate limits when the event type and reason match.
func (c *Client) Eventf(object runtime.Object, eventType, reason, messageFmt string, args ...any) {
	if !c.allow(object, eventType, reason) {
		return
	}
	c.delegate.Eventf(object, eventType, reason, messageFmt, args...)
}

// AnnotatedEventf records a formatted event with annotations, applying configured rate limits when
// the event type and reason match.
func (c *Client) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventType, reason, messageFmt string, args ...any) {
	if !c.allow(object, eventType, reason) {
		return
	}
	c.delegate.AnnotatedEventf(object, annotations, eventType, reason, messageFmt, args...)
}

// CreateOrPatchSuccess records a Normal success event for a CreateOrPatch reconcile result.
//
// Actual state changes (OperationResultCreated / OperationResultUpdated) are always recorded.
// Steady-state reconciles return OperationResultNone and are throttled according to Config rules
// for Normal events with the given reason.
func (c *Client) CreateOrPatchSuccess(object runtime.Object, opResult controllerutil.OperationResult, reason, messageFmt string, args ...any) {
	if opResult != controllerutil.OperationResultNone {
		c.delegate.Eventf(object, corev1.EventTypeNormal, reason, messageFmt, args...)
		return
	}
	c.Eventf(object, corev1.EventTypeNormal, reason, messageFmt, args...)
}

func (c *Client) allow(object runtime.Object, eventType, reason string) bool {
	key := rateLimitKey(object, reason)
	for i, rule := range c.config.Rules {
		if rule.EventType != eventType {
			continue
		}
		if len(rule.Reasons) > 0 && !containsReason(rule.Reasons, reason) {
			continue
		}
		limiter := c.limiters[i]
		if limiter == nil {
			continue
		}
		if !limiter.allow(key) {
			return false
		}
	}
	return true
}

func containsReason(reasons []string, reason string) bool {
	for _, candidate := range reasons {
		if candidate == reason {
			return true
		}
	}
	return false
}
