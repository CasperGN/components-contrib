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
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	kubeclient "github.com/dapr/components-contrib/common/authentication/kubernetes"
	"github.com/dapr/components-contrib/configuration"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

var _ configuration.Store = (*ConfigurationStore)(nil)

// ConfigurationStore implements a Kubernetes ConfigMap-backed configuration store.
// A single shared informer watches the configured ConfigMap and fans out change
// events to all registered subscribers, avoiding per-subscription watches against
// the API server.
type ConfigurationStore struct {
	kubeClient kubernetes.Interface
	metadata   metadata
	logger     logger.Logger

	registry *subscriptionRegistry

	// Shared informer lifecycle: started in Init, stopped in Close.
	informerCancel context.CancelFunc
	informerWg     sync.WaitGroup

	lock   sync.RWMutex
	closed atomic.Bool
}

// NewKubernetesConfigMapStore returns a new Kubernetes ConfigMap configuration store.
func NewKubernetesConfigMapStore(logger logger.Logger) configuration.Store {
	return &ConfigurationStore{
		logger:   logger,
		registry: newSubscriptionRegistry(),
	}
}

func (s *ConfigurationStore) Init(_ context.Context, meta configuration.Metadata) error {
	if err := s.metadata.parse(meta); err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}

	if s.kubeClient == nil {
		kubeconfigPath := ""
		if s.metadata.KubeconfigPath != nil {
			kubeconfigPath = *s.metadata.KubeconfigPath
		} else {
			kubeconfigPath = kubeclient.GetKubeconfigPath(s.logger, os.Args)
		}

		client, err := kubeclient.GetKubeClient(kubeconfigPath)
		if err != nil {
			return fmt.Errorf("failed to create Kubernetes client: %w", err)
		}
		s.kubeClient = client
	}

	s.startInformer()

	return nil
}

func (s *ConfigurationStore) Get(ctx context.Context, req *configuration.GetRequest) (*configuration.GetResponse, error) {
	ns := s.resolveNamespace(req.Metadata)
	cm, err := s.kubeClient.CoreV1().ConfigMaps(ns).Get(ctx, s.metadata.ConfigMapName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get ConfigMap %q: %w", s.metadata.ConfigMapName, err)
	}

	items := make(map[string]*configuration.Item)

	if len(req.Keys) == 0 {
		for k, v := range cm.Data {
			items[k] = &configuration.Item{
				Value:    v,
				Version:  cm.ResourceVersion,
				Metadata: map[string]string{},
			}
		}
		for k, v := range cm.BinaryData {
			items[k] = &configuration.Item{
				Value:    base64.StdEncoding.EncodeToString(v),
				Version:  cm.ResourceVersion,
				Metadata: map[string]string{"encoding": "base64"},
			}
		}
	} else {
		for _, key := range req.Keys {
			if v, ok := cm.Data[key]; ok {
				items[key] = &configuration.Item{
					Value:    v,
					Version:  cm.ResourceVersion,
					Metadata: map[string]string{},
				}
			} else if v, ok := cm.BinaryData[key]; ok {
				items[key] = &configuration.Item{
					Value:    base64.StdEncoding.EncodeToString(v),
					Version:  cm.ResourceVersion,
					Metadata: map[string]string{"encoding": "base64"},
				}
			}
		}
	}

	return &configuration.GetResponse{Items: items}, nil
}

func (s *ConfigurationStore) Subscribe(ctx context.Context, req *configuration.SubscribeRequest, handler configuration.UpdateHandler) (string, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if s.closed.Load() {
		return "", errors.New("configuration store is closed")
	}

	subscribeID := uuid.New().String()
	childCtx, cancel := context.WithCancel(ctx)

	sub := &subscriber{
		id:      subscribeID,
		keys:    req.Keys,
		handler: handler,
		ctx:     childCtx,
		cancel:  cancel,
	}
	s.registry.add(sub)

	// Deliver current state asynchronously. The Dapr sidecar's gRPC handler
	// blocks until Subscribe returns (to send the subscription ID first), so
	// calling the handler synchronously here would deadlock.
	go func() {
		resp, err := s.Get(ctx, &configuration.GetRequest{
			Keys:     req.Keys,
			Metadata: req.Metadata,
		})
		if err == nil && len(resp.Items) > 0 {
			if hErr := handler(ctx, &configuration.UpdateEvent{
				ID:    subscribeID,
				Items: resp.Items,
			}); hErr != nil {
				s.logger.Errorf("failed to send initial state for subscription %s: %v", subscribeID, hErr)
			}
		}
	}()

	return subscribeID, nil
}

// startInformer starts a single informer that watches the configured ConfigMap
// and fans out change events to all registered subscribers.
func (s *ConfigurationStore) startInformer() {
	// The informer must outlive the Init context (which is request-scoped), so
	// we create a dedicated context controlled by s.informerCancel that lives
	// until Close() is called.
	ctx, cancel := context.WithCancel(context.Background())
	s.informerCancel = cancel

	ns := s.resolveNamespace(nil)

	var resyncPeriod time.Duration
	if s.metadata.ResyncPeriod != nil {
		resyncPeriod = *s.metadata.ResyncPeriod
	}

	watchlist := cache.NewFilteredListWatchFromClient(
		s.kubeClient.CoreV1().RESTClient(),
		"configmaps",
		ns,
		func(options *metav1.ListOptions) {
			options.FieldSelector = fields.OneTermEqualSelector("metadata.name", s.metadata.ConfigMapName).String()
		},
	)

	_, controller := cache.NewInformerWithOptions(cache.InformerOptions{
		ListerWatcher: watchlist,
		ObjectType:    &corev1.ConfigMap{},
		ResyncPeriod:  resyncPeriod,
		Handler: cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(oldObj, newObj any) {
				s.fanOutUpdate(oldObj, newObj)
			},
		},
	})

	s.informerWg.Add(1)
	go func() {
		defer s.informerWg.Done()
		controller.Run(ctx.Done())
		s.logger.Info("ConfigMap informer stopped")
	}()
}

// fanOutUpdate dispatches ConfigMap change events to all registered subscribers.
func (s *ConfigurationStore) fanOutUpdate(oldObj, newObj any) {
	oldCM, ok1 := oldObj.(*corev1.ConfigMap)
	newCM, ok2 := newObj.(*corev1.ConfigMap)
	if !ok1 || !ok2 {
		s.logger.Warn("received non-ConfigMap object in update handler")
		return
	}

	// Skip no-op updates.
	if oldCM.ResourceVersion == newCM.ResourceVersion {
		return
	}
	if maps.Equal(oldCM.Data, newCM.Data) &&
		maps.EqualFunc(oldCM.BinaryData, newCM.BinaryData, bytes.Equal) {
		return
	}

	// Compute all changed items once, then filter per subscriber.
	allChanged := computeChangedItems(oldCM, newCM)
	if len(allChanged) == 0 {
		return
	}

	s.registry.mu.RLock()
	defer s.registry.mu.RUnlock()

	// Build per-subscriber item sets using the key index.
	perSub := make(map[string]map[string]*configuration.Item)

	for id := range s.registry.allKeys {
		perSub[id] = allChanged
	}

	for changedKey, item := range allChanged {
		if subs, ok := s.registry.byKey[changedKey]; ok {
			for id := range subs {
				if _, exists := perSub[id]; !exists {
					perSub[id] = make(map[string]*configuration.Item)
				}
				perSub[id][changedKey] = item
			}
		}
	}

	// Dispatch to each subscriber concurrently.
	var wg sync.WaitGroup
	for subID, items := range perSub {
		sub := s.registry.byID[subID]
		if sub.ctx.Err() != nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sub.handler(sub.ctx, &configuration.UpdateEvent{
				ID:    subID,
				Items: items,
			}); err != nil {
				s.logger.Errorf("subscription %s handler error: %v", subID, err)
			}
		}()
	}
	wg.Wait()
}

// computeChangedItems computes the set of configuration items that changed
// between oldCM and newCM across both Data and BinaryData.
func computeChangedItems(oldCM, newCM *corev1.ConfigMap) map[string]*configuration.Item {
	changedItems := make(map[string]*configuration.Item)

	// Detect added or modified keys in data.
	for k, newVal := range newCM.Data {
		oldVal, existed := oldCM.Data[k]
		if !existed || oldVal != newVal {
			changedItems[k] = &configuration.Item{
				Value:    newVal,
				Version:  newCM.ResourceVersion,
				Metadata: map[string]string{},
			}
		}
	}

	// Detect added or modified keys in binaryData.
	for k, newVal := range newCM.BinaryData {
		oldVal, existed := oldCM.BinaryData[k]
		if !existed || !bytes.Equal(oldVal, newVal) {
			changedItems[k] = &configuration.Item{
				Value:    base64.StdEncoding.EncodeToString(newVal),
				Version:  newCM.ResourceVersion,
				Metadata: map[string]string{"encoding": "base64"},
			}
		}
	}

	// Detect deleted keys: a key is deleted only if it existed in old data or
	// binaryData and is now absent from BOTH new data and binaryData. This
	// prevents a false deletion when a key moves between data and binaryData.
	allOldKeys := make(map[string]struct{})
	for k := range oldCM.Data {
		allOldKeys[k] = struct{}{}
	}
	for k := range oldCM.BinaryData {
		allOldKeys[k] = struct{}{}
	}
	for k := range allOldKeys {
		_, inNewData := newCM.Data[k]
		_, inNewBinary := newCM.BinaryData[k]
		if !inNewData && !inNewBinary {
			changedItems[k] = &configuration.Item{
				Value:    "",
				Version:  newCM.ResourceVersion,
				Metadata: map[string]string{"deleted": "true"},
			}
		}
	}

	return changedItems
}

func (s *ConfigurationStore) Unsubscribe(_ context.Context, req *configuration.UnsubscribeRequest) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if sub, ok := s.registry.remove(req.ID); ok {
		sub.cancel()
		return nil
	}

	return fmt.Errorf("subscription with id %s does not exist", req.ID)
}

func (s *ConfigurationStore) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := metadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.ConfigurationStoreType)
	return metadataInfo
}

func (s *ConfigurationStore) Close() error {
	s.closed.Store(true)

	s.lock.Lock()
	defer s.lock.Unlock()

	s.registry.cancelAll()

	if s.informerCancel != nil {
		s.informerCancel()
	}
	s.informerWg.Wait()

	return nil
}

func (s *ConfigurationStore) resolveNamespace(requestMetadata map[string]string) string {
	if ns, ok := requestMetadata["namespace"]; ok && ns != "" {
		return ns
	}
	if s.metadata.Namespace != nil {
		return *s.metadata.Namespace
	}
	if ns, ok := os.LookupEnv("NAMESPACE"); ok {
		return ns
	}
	return "default"
}
