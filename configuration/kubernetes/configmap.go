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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
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
	namespace  string
	logger     logger.Logger

	registry *subscriptionRegistry

	// Shared informer lifecycle: started in Init, stopped in Close.
	informerStore  cache.Store
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

func (s *ConfigurationStore) Init(ctx context.Context, meta configuration.Metadata) error {
	if err := s.metadata.parse(meta); err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}

	s.namespace = resolveNamespace()

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

	if err := s.startInformer(ctx); err != nil {
		return fmt.Errorf("failed to start informer: %w", err)
	}

	return nil
}

func (s *ConfigurationStore) Get(_ context.Context, req *configuration.GetRequest) (*configuration.GetResponse, error) {
	obj, exists, err := s.informerStore.GetByKey(s.namespace + "/" + s.metadata.ConfigMapName)
	if err != nil {
		return nil, fmt.Errorf("failed to get ConfigMap %q from cache: %w", s.metadata.ConfigMapName, err)
	}
	if !exists {
		return &configuration.GetResponse{Items: map[string]*configuration.Item{}}, nil
	}

	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		return nil, fmt.Errorf("unexpected object type in cache for ConfigMap %q", s.metadata.ConfigMapName)
	}

	allItems := configMapToItems(cm)

	if len(req.Keys) == 0 {
		return &configuration.GetResponse{Items: allItems}, nil
	}

	items := make(map[string]*configuration.Item, len(req.Keys))
	for _, key := range req.Keys {
		if item, ok := allItems[key]; ok {
			items[key] = item
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
	// Read initial state from cache (instant, no API call).
	resp, err := s.Get(childCtx, &configuration.GetRequest{
		Keys:     req.Keys,
		Metadata: req.Metadata,
	})
	if err != nil {
		cancel()
		return "", fmt.Errorf("failed to get initial state: %w", err)
	}

	s.registry.add(sub)

	// Deliver initial state asynchronously. The Dapr sidecar's gRPC handler
	// blocks until Subscribe returns (to send the subscription ID first), so
	// calling the handler synchronously here would deadlock.
	if len(resp.Items) > 0 {
		go func() {
			if hErr := handler(childCtx, &configuration.UpdateEvent{
				ID:    subscribeID,
				Items: resp.Items,
			}); hErr != nil {
				s.logger.Errorf("failed to send initial state for subscription %s: %v", subscribeID, hErr)
			}
		}()
	}

	return subscribeID, nil
}

// startInformer starts a single informer that watches the configured ConfigMap
// and fans out change events to all registered subscribers.
func (s *ConfigurationStore) startInformer(ctx context.Context) error {
	// The informer must outlive the Init context (which is request-scoped), so
	// we create a dedicated context controlled by s.informerCancel that lives
	// until Close() is called.
	informerCtx, cancel := context.WithCancel(context.Background())
	s.informerCancel = cancel

	var resyncPeriod time.Duration
	if s.metadata.ResyncPeriod != nil {
		resyncPeriod = *s.metadata.ResyncPeriod
	}

	fieldSelector := fields.OneTermEqualSelector("metadata.name", s.metadata.ConfigMapName).String()
	watchlist := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			options.FieldSelector = fieldSelector
			return s.kubeClient.CoreV1().ConfigMaps(s.namespace).List(informerCtx, options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.FieldSelector = fieldSelector
			return s.kubeClient.CoreV1().ConfigMaps(s.namespace).Watch(informerCtx, options)
		},
	}

	store, controller := cache.NewInformerWithOptions(cache.InformerOptions{
		ListerWatcher: watchlist,
		ObjectType:    &corev1.ConfigMap{},
		ResyncPeriod:  resyncPeriod,
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {
				s.fanOutAdd(obj)
			},
			UpdateFunc: func(oldObj, newObj any) {
				s.fanOutUpdate(oldObj, newObj)
			},
			DeleteFunc: func(obj any) {
				s.fanOutDelete(obj)
			},
		},
	})

	s.informerStore = store

	s.informerWg.Add(1)
	go func() {
		defer s.informerWg.Done()
		controller.Run(informerCtx.Done())
		s.logger.Info("ConfigMap informer stopped")
	}()

	// Use Init's context for the sync check — it provides a timeout bound.
	if !cache.WaitForCacheSync(ctx.Done(), controller.HasSynced) {
		cancel()
		s.informerWg.Wait()
		return fmt.Errorf("failed to sync informer cache for ConfigMap %q", s.metadata.ConfigMapName)
	}

	return nil
}

// fanOutAdd dispatches all items from a newly created ConfigMap to subscribers.
func (s *ConfigurationStore) fanOutAdd(obj any) {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		s.logger.Warn("received non-ConfigMap object in add handler")
		return
	}

	items := configMapToItems(cm)
	if len(items) == 0 {
		return
	}

	s.dispatchToSubscribers(items)
}

// fanOutDelete notifies subscribers that all keys have been deleted when the
// ConfigMap is removed from the cluster.
func (s *ConfigurationStore) fanOutDelete(obj any) {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		s.logger.Warn("received non-ConfigMap object in delete handler")
		return
	}

	items := make(map[string]*configuration.Item, len(cm.Data)+len(cm.BinaryData))
	for k := range cm.Data {
		items[k] = &configuration.Item{
			Value:    "",
			Version:  cm.ResourceVersion,
			Metadata: map[string]string{"deleted": "true"},
		}
	}
	for k := range cm.BinaryData {
		items[k] = &configuration.Item{
			Value:    "",
			Version:  cm.ResourceVersion,
			Metadata: map[string]string{"deleted": "true"},
		}
	}

	if len(items) == 0 {
		return
	}

	s.dispatchToSubscribers(items)
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

	allChanged := computeChangedItems(oldCM, newCM)
	if len(allChanged) == 0 {
		return
	}

	s.dispatchToSubscribers(allChanged)
}

// dispatchToSubscribers snapshots subscribers under a read lock, then dispatches
// items concurrently without holding the lock. This prevents deadlock if a
// handler calls Unsubscribe.
func (s *ConfigurationStore) dispatchToSubscribers(allItems map[string]*configuration.Item) {
	type subSnapshot struct {
		id      string
		handler configuration.UpdateHandler
		ctx     context.Context
		items   map[string]*configuration.Item
	}

	// Snapshot under lock.
	s.registry.mu.RLock()

	snapshots := make([]subSnapshot, 0, len(s.registry.byID))

	for _, sub := range s.registry.allKeys {
		if sub.ctx.Err() != nil {
			continue
		}
		itemsCopy := make(map[string]*configuration.Item, len(allItems))
		maps.Copy(itemsCopy, allItems)
		snapshots = append(snapshots, subSnapshot{
			id: sub.id, handler: sub.handler, ctx: sub.ctx, items: itemsCopy,
		})
	}

	for changedKey, item := range allItems {
		if subs, ok := s.registry.byKey[changedKey]; ok {
			for _, sub := range subs {
				if sub.ctx.Err() != nil {
					continue
				}
				// Find or create this subscriber's snapshot.
				found := false
				for i := range snapshots {
					if snapshots[i].id == sub.id {
						snapshots[i].items[changedKey] = item
						found = true
						break
					}
				}
				if !found {
					snapshots = append(snapshots, subSnapshot{
						id: sub.id, handler: sub.handler, ctx: sub.ctx,
						items: map[string]*configuration.Item{changedKey: item},
					})
				}
			}
		}
	}

	s.registry.mu.RUnlock()

	// Dispatch without holding any lock.
	var wg sync.WaitGroup
	for _, snap := range snapshots {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := snap.handler(snap.ctx, &configuration.UpdateEvent{
				ID:    snap.id,
				Items: snap.items,
			}); err != nil {
				s.logger.Errorf("subscription %s handler error: %v", snap.id, err)
			}
		}()
	}
	wg.Wait()
}

// configMapToItems builds configuration items from all data in a ConfigMap.
func configMapToItems(cm *corev1.ConfigMap) map[string]*configuration.Item {
	items := make(map[string]*configuration.Item, len(cm.Data)+len(cm.BinaryData))
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
	return items
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

func resolveNamespace() string {
	if ns, ok := os.LookupEnv("NAMESPACE"); ok {
		return ns
	}
	return "default"
}
