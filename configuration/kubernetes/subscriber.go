/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kubernetes

import (
	"context"
	"sync"

	"github.com/dapr/components-contrib/configuration"
)

// subscriber holds the state for a single subscription registered via Subscribe.
type subscriber struct {
	id      string
	keys    []string // empty means all keys
	handler configuration.UpdateHandler
	ctx     context.Context
	cancel  context.CancelFunc
}

// subscriptionRegistry indexes subscribers both by ID (for unsubscribe) and by
// key (for efficient fan-out). Subscribers watching all keys are stored in the
// allKeys map so that every change event reaches them without per-key iteration.
type subscriptionRegistry struct {
	mu      sync.RWMutex
	byID    map[string]*subscriber            // subID -> subscriber
	byKey   map[string]map[string]*subscriber // configKey -> {subID -> subscriber}
	allKeys map[string]*subscriber            // subscribers watching all keys
}

func newSubscriptionRegistry() *subscriptionRegistry {
	return &subscriptionRegistry{
		byID:    make(map[string]*subscriber),
		byKey:   make(map[string]map[string]*subscriber),
		allKeys: make(map[string]*subscriber),
	}
}

func (r *subscriptionRegistry) add(sub *subscriber) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.byID[sub.id] = sub

	if len(sub.keys) == 0 {
		r.allKeys[sub.id] = sub
		return
	}
	for _, key := range sub.keys {
		if r.byKey[key] == nil {
			r.byKey[key] = make(map[string]*subscriber)
		}
		r.byKey[key][sub.id] = sub
	}
}

func (r *subscriptionRegistry) remove(id string) (*subscriber, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	sub, ok := r.byID[id]
	if !ok {
		return nil, false
	}
	delete(r.byID, id)

	if len(sub.keys) == 0 {
		delete(r.allKeys, id)
	} else {
		for _, key := range sub.keys {
			if subs, exists := r.byKey[key]; exists {
				delete(subs, id)
				if len(subs) == 0 {
					delete(r.byKey, key)
				}
			}
		}
	}
	return sub, true
}

func (r *subscriptionRegistry) cancelAll() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, sub := range r.byID {
		sub.cancel()
	}
	r.byID = make(map[string]*subscriber)
	r.byKey = make(map[string]map[string]*subscriber)
	r.allKeys = make(map[string]*subscriber)
}
