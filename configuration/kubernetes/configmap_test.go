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
	"encoding/base64"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/dapr/components-contrib/configuration"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

func newTestStore(t *testing.T, objects ...corev1.ConfigMap) *ConfigurationStore {
	t.Helper()

	fakeClient := fake.NewSimpleClientset()
	for i := range objects {
		_, err := fakeClient.CoreV1().ConfigMaps(objects[i].Namespace).Create(
			t.Context(), &objects[i], metav1.CreateOptions{},
		)
		require.NoError(t, err)
	}

	return &ConfigurationStore{
		kubeClient: fakeClient,
		logger:     logger.NewLogger("test"),
		registry:   newSubscriptionRegistry(),
	}
}

func testConfigMap() corev1.ConfigMap {
	return corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "my-config",
			Namespace:       "default",
			ResourceVersion: "100",
		},
		Data: map[string]string{
			"log.level":          "info",
			"feature.enable-v2":  "true",
			"database.pool-size": "10",
		},
	}
}

func TestNewKubernetesConfigMapStore(t *testing.T) {
	store := NewKubernetesConfigMapStore(logger.NewLogger("test"))
	assert.NotNil(t, store)
}

func TestMetadata_Parse(t *testing.T) {
	t.Run("valid metadata", func(t *testing.T) {
		var m metadata
		err := m.parse(configuration.Metadata{
			Base: contribMetadata.Base{
				Properties: map[string]string{
					"configMapName": "my-config",
					"namespace":     "production",
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "my-config", m.ConfigMapName)
		require.NotNil(t, m.Namespace)
		assert.Equal(t, "production", *m.Namespace)
	})

	t.Run("missing configMapName returns error", func(t *testing.T) {
		var m metadata
		err := m.parse(configuration.Metadata{
			Base: contribMetadata.Base{
				Properties: map[string]string{
					"namespace": "default",
				},
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "configMapName is required")
	})

	t.Run("namespace nil when not set", func(t *testing.T) {
		var m metadata
		err := m.parse(configuration.Metadata{
			Base: contribMetadata.Base{
				Properties: map[string]string{
					"configMapName": "my-config",
				},
			},
		})
		require.NoError(t, err)
		assert.Nil(t, m.Namespace)
	})

	t.Run("dotted configMapName is valid (DNS subdomain)", func(t *testing.T) {
		var m metadata
		err := m.parse(configuration.Metadata{
			Base: contribMetadata.Base{
				Properties: map[string]string{
					"configMapName": "my.dotted.config",
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "my.dotted.config", m.ConfigMapName)
	})

	t.Run("invalid configMapName is rejected", func(t *testing.T) {
		var m metadata
		err := m.parse(configuration.Metadata{
			Base: contribMetadata.Base{
				Properties: map[string]string{
					"configMapName": "INVALID_NAME",
				},
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a valid Kubernetes resource name")
	})

	t.Run("dotted namespace is rejected (must be DNS label)", func(t *testing.T) {
		var m metadata
		err := m.parse(configuration.Metadata{
			Base: contribMetadata.Base{
				Properties: map[string]string{
					"configMapName": "my-config",
					"namespace":     "my.namespace",
				},
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a valid Kubernetes namespace name")
	})

	t.Run("invalid namespace is rejected", func(t *testing.T) {
		var m metadata
		err := m.parse(configuration.Metadata{
			Base: contribMetadata.Base{
				Properties: map[string]string{
					"configMapName": "my-config",
					"namespace":     "../../etc",
				},
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a valid Kubernetes namespace name")
	})

	t.Run("resyncPeriod is parsed", func(t *testing.T) {
		var m metadata
		err := m.parse(configuration.Metadata{
			Base: contribMetadata.Base{
				Properties: map[string]string{
					"configMapName": "my-config",
					"resyncPeriod":  "10m",
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, m.ResyncPeriod)
		assert.Equal(t, 10*time.Minute, *m.ResyncPeriod)
	})

	t.Run("resyncPeriod nil when not set", func(t *testing.T) {
		var m metadata
		err := m.parse(configuration.Metadata{
			Base: contribMetadata.Base{
				Properties: map[string]string{
					"configMapName": "my-config",
				},
			},
		})
		require.NoError(t, err)
		assert.Nil(t, m.ResyncPeriod)
	})
}

func TestInit_ValidMetadata(t *testing.T) {
	store := newTestStore(t)

	err := store.Init(t.Context(), configuration.Metadata{
		Base: contribMetadata.Base{
			Properties: map[string]string{
				"configMapName": "my-config",
				"namespace":     "default",
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "my-config", store.metadata.ConfigMapName)
	require.NotNil(t, store.metadata.Namespace)
	assert.Equal(t, "default", *store.metadata.Namespace)

	// Clean up informer started by Init.
	require.NoError(t, store.Close())
}

func TestInit_InvalidMetadata(t *testing.T) {
	store := newTestStore(t)

	err := store.Init(t.Context(), configuration.Metadata{
		Base: contribMetadata.Base{
			Properties: map[string]string{
				"namespace": "default",
			},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "configMapName is required")
}

func TestGet_AllKeys(t *testing.T) {
	cm := testConfigMap()
	store := newTestStore(t, cm)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: ptr.Of("default")}

	resp, err := store.Get(t.Context(), &configuration.GetRequest{
		Keys:     []string{},
		Metadata: map[string]string{},
	})
	require.NoError(t, err)
	assert.Len(t, resp.Items, 3)
	assert.Equal(t, "info", resp.Items["log.level"].Value)
	assert.Equal(t, "true", resp.Items["feature.enable-v2"].Value)
	assert.Equal(t, "10", resp.Items["database.pool-size"].Value)
}

func TestGet_SpecificKeys(t *testing.T) {
	cm := testConfigMap()
	store := newTestStore(t, cm)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: ptr.Of("default")}

	resp, err := store.Get(t.Context(), &configuration.GetRequest{
		Keys:     []string{"log.level"},
		Metadata: map[string]string{},
	})
	require.NoError(t, err)
	assert.Len(t, resp.Items, 1)
	assert.Equal(t, "info", resp.Items["log.level"].Value)
}

func TestGet_MissingKeys(t *testing.T) {
	cm := testConfigMap()
	store := newTestStore(t, cm)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: ptr.Of("default")}

	resp, err := store.Get(t.Context(), &configuration.GetRequest{
		Keys:     []string{"nonexistent.key"},
		Metadata: map[string]string{},
	})
	require.NoError(t, err)
	assert.Empty(t, resp.Items)
}

func TestGet_BinaryData(t *testing.T) {
	cm := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "my-config",
			Namespace:       "default",
			ResourceVersion: "100",
		},
		Data: map[string]string{
			"text-key": "hello",
		},
		BinaryData: map[string][]byte{
			"binary-key": {0x01, 0x02, 0x03},
		},
	}
	store := newTestStore(t, cm)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: ptr.Of("default")}

	t.Run("get all includes binary data", func(t *testing.T) {
		resp, err := store.Get(t.Context(), &configuration.GetRequest{
			Keys:     []string{},
			Metadata: map[string]string{},
		})
		require.NoError(t, err)
		assert.Len(t, resp.Items, 2)

		assert.Equal(t, "hello", resp.Items["text-key"].Value)
		assert.Empty(t, resp.Items["text-key"].Metadata["encoding"])

		expectedB64 := base64.StdEncoding.EncodeToString([]byte{0x01, 0x02, 0x03})
		assert.Equal(t, expectedB64, resp.Items["binary-key"].Value)
		assert.Equal(t, "base64", resp.Items["binary-key"].Metadata["encoding"])
	})

	t.Run("get specific binary key", func(t *testing.T) {
		resp, err := store.Get(t.Context(), &configuration.GetRequest{
			Keys:     []string{"binary-key"},
			Metadata: map[string]string{},
		})
		require.NoError(t, err)
		assert.Len(t, resp.Items, 1)
		assert.Equal(t, "base64", resp.Items["binary-key"].Metadata["encoding"])
	})
}

func TestGet_EmptyConfigMap(t *testing.T) {
	cm := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-config",
			Namespace: "default",
		},
	}
	store := newTestStore(t, cm)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: ptr.Of("default")}

	resp, err := store.Get(t.Context(), &configuration.GetRequest{
		Keys:     []string{},
		Metadata: map[string]string{},
	})
	require.NoError(t, err)
	assert.Empty(t, resp.Items)
}

func TestGet_ConfigMapNotFound(t *testing.T) {
	store := newTestStore(t)
	store.metadata = metadata{ConfigMapName: "missing", Namespace: ptr.Of("default")}

	_, err := store.Get(t.Context(), &configuration.GetRequest{
		Keys:     []string{},
		Metadata: map[string]string{},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get ConfigMap")
}

func TestUnsubscribe_Valid(t *testing.T) {
	store := newTestStore(t)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: ptr.Of("default")}

	cancelled := false
	store.registry.add(&subscriber{
		id:     "test-sub-id",
		ctx:    t.Context(),
		cancel: func() { cancelled = true },
		keys:   []string{},
		handler: func(_ context.Context, _ *configuration.UpdateEvent) error {
			return nil
		},
	})

	err := store.Unsubscribe(t.Context(), &configuration.UnsubscribeRequest{ID: "test-sub-id"})
	require.NoError(t, err)
	assert.True(t, cancelled)
}

func TestUnsubscribe_InvalidID(t *testing.T) {
	store := newTestStore(t)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: ptr.Of("default")}

	err := store.Unsubscribe(t.Context(), &configuration.UnsubscribeRequest{ID: "nonexistent"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestClose_CancelsAllSubscriptions(t *testing.T) {
	store := newTestStore(t)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: ptr.Of("default")}

	var mu sync.Mutex
	cancelledIDs := []string{}

	for _, id := range []string{"sub-1", "sub-2", "sub-3"} {
		capturedID := id
		store.registry.add(&subscriber{
			id:  id,
			ctx: t.Context(),
			cancel: func() {
				mu.Lock()
				cancelledIDs = append(cancelledIDs, capturedID)
				mu.Unlock()
			},
			keys: []string{},
			handler: func(_ context.Context, _ *configuration.UpdateEvent) error {
				return nil
			},
		})
	}

	err := store.Close()
	require.NoError(t, err)
	assert.Len(t, cancelledIDs, 3)
	assert.ElementsMatch(t, []string{"sub-1", "sub-2", "sub-3"}, cancelledIDs)
}

func TestClose_PreventsFurtherSubscriptions(t *testing.T) {
	cm := testConfigMap()
	store := newTestStore(t, cm)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: ptr.Of("default")}

	err := store.Close()
	require.NoError(t, err)

	_, err = store.Subscribe(t.Context(), &configuration.SubscribeRequest{
		Keys:     []string{"log.level"},
		Metadata: map[string]string{},
	}, func(_ context.Context, _ *configuration.UpdateEvent) error {
		return nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestNamespaceResolution(t *testing.T) {
	t.Run("request metadata takes precedence", func(t *testing.T) {
		store := newTestStore(t)
		store.metadata = metadata{ConfigMapName: "my-config", Namespace: ptr.Of("component-ns")}

		ns := store.resolveNamespace(map[string]string{"namespace": "request-ns"})
		assert.Equal(t, "request-ns", ns)
	})

	t.Run("env var fallback when namespace not set", func(t *testing.T) {
		store := newTestStore(t)
		store.metadata = metadata{ConfigMapName: "my-config"}

		t.Setenv("NAMESPACE", "env-ns")
		ns := store.resolveNamespace(map[string]string{})
		assert.Equal(t, "env-ns", ns)
	})

	t.Run("env var ignored when namespace is explicit", func(t *testing.T) {
		store := newTestStore(t)
		store.metadata = metadata{ConfigMapName: "my-config", Namespace: ptr.Of("component-ns")}

		t.Setenv("NAMESPACE", "env-ns")
		ns := store.resolveNamespace(map[string]string{})
		assert.Equal(t, "component-ns", ns)
	})

	t.Run("component metadata namespace used", func(t *testing.T) {
		store := newTestStore(t)
		store.metadata = metadata{ConfigMapName: "my-config", Namespace: ptr.Of("component-ns")}

		ns := store.resolveNamespace(map[string]string{})
		assert.Equal(t, "component-ns", ns)
	})

	t.Run("defaults to 'default' when nothing set", func(t *testing.T) {
		store := newTestStore(t)
		store.metadata = metadata{ConfigMapName: "my-config"}

		ns := store.resolveNamespace(map[string]string{})
		assert.Equal(t, "default", ns)
	})

	t.Run("namespace override passes through to API server", func(t *testing.T) {
		store := newTestStore(t)
		store.metadata = metadata{ConfigMapName: "my-config", Namespace: ptr.Of("component-ns")}

		// Invalid namespace is no longer validated client-side; the K8s API
		// server handles validation.
		ns := store.resolveNamespace(map[string]string{"namespace": "../../etc"})
		assert.Equal(t, "../../etc", ns)
	})
}

func TestGetComponentMetadata(t *testing.T) {
	store := &ConfigurationStore{
		logger:   logger.NewLogger("test"),
		registry: newSubscriptionRegistry(),
	}
	metadataInfo := store.GetComponentMetadata()
	assert.NotNil(t, metadataInfo)
}

func TestComputeChangedItems(t *testing.T) {
	t.Run("detects added and modified keys", func(t *testing.T) {
		oldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			Data:       map[string]string{"key1": "old-val"},
		}
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"},
			Data:       map[string]string{"key1": "new-val", "key2": "added"},
		}

		items := computeChangedItems(oldCM, newCM)

		assert.Len(t, items, 2)
		assert.Equal(t, "new-val", items["key1"].Value)
		assert.Equal(t, "added", items["key2"].Value)
		assert.Equal(t, "2", items["key1"].Version)
	})

	t.Run("detects deleted keys with metadata", func(t *testing.T) {
		oldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			Data:       map[string]string{"key1": "val1", "key2": "val2"},
		}
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"},
			Data:       map[string]string{"key1": "val1"},
		}

		items := computeChangedItems(oldCM, newCM)

		assert.Len(t, items, 1)
		assert.Equal(t, "", items["key2"].Value)
		assert.Equal(t, "true", items["key2"].Metadata["deleted"])
	})

	t.Run("no changes when data is identical", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			Data:       map[string]string{"key1": "val1"},
		}

		items := computeChangedItems(cm, cm)

		assert.Empty(t, items)
	})

	t.Run("detects binaryData changes", func(t *testing.T) {
		oldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			BinaryData: map[string][]byte{"bin-key": {0x01}},
		}
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"},
			BinaryData: map[string][]byte{"bin-key": {0x01, 0x02}},
		}

		items := computeChangedItems(oldCM, newCM)

		assert.Len(t, items, 1)
		expected := base64.StdEncoding.EncodeToString([]byte{0x01, 0x02})
		assert.Equal(t, expected, items["bin-key"].Value)
		assert.Equal(t, "base64", items["bin-key"].Metadata["encoding"])
	})

	t.Run("detects deleted binaryData keys", func(t *testing.T) {
		oldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			BinaryData: map[string][]byte{"bin-key": {0x01}},
		}
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"},
			BinaryData: map[string][]byte{},
		}

		items := computeChangedItems(oldCM, newCM)

		assert.Equal(t, "", items["bin-key"].Value)
		assert.Equal(t, "true", items["bin-key"].Metadata["deleted"])
	})

	t.Run("key moved from binaryData to data is not marked deleted", func(t *testing.T) {
		oldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			BinaryData: map[string][]byte{"shared-key": {0x01}},
		}
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"},
			Data:       map[string]string{"shared-key": "now-text"},
		}

		items := computeChangedItems(oldCM, newCM)

		assert.Len(t, items, 1)
		assert.Equal(t, "now-text", items["shared-key"].Value)
		assert.Empty(t, items["shared-key"].Metadata["deleted"])
	})
}

func TestFanOutUpdate(t *testing.T) {
	t.Run("dispatches to multiple subscribers", func(t *testing.T) {
		store := newTestStore(t)
		store.metadata = metadata{ConfigMapName: "my-config", Namespace: ptr.Of("default")}

		var mu sync.Mutex
		receivedByID := map[string]*configuration.UpdateEvent{}

		for _, id := range []string{"sub-1", "sub-2"} {
			capturedID := id
			store.registry.add(&subscriber{
				id:     id,
				ctx:    t.Context(),
				cancel: func() {},
				keys:   []string{},
				handler: func(_ context.Context, e *configuration.UpdateEvent) error {
					mu.Lock()
					receivedByID[capturedID] = e
					mu.Unlock()
					return nil
				},
			})
		}

		oldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			Data:       map[string]string{"key1": "old"},
		}
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"},
			Data:       map[string]string{"key1": "new"},
		}

		store.fanOutUpdate(oldCM, newCM)

		assert.Len(t, receivedByID, 2)
		assert.Equal(t, "sub-1", receivedByID["sub-1"].ID)
		assert.Equal(t, "sub-2", receivedByID["sub-2"].ID)
		assert.Equal(t, "new", receivedByID["sub-1"].Items["key1"].Value)
	})

	t.Run("filters per subscriber key set", func(t *testing.T) {
		store := newTestStore(t)
		store.metadata = metadata{ConfigMapName: "my-config", Namespace: ptr.Of("default")}

		var mu sync.Mutex
		receivedByID := map[string]*configuration.UpdateEvent{}

		store.registry.add(&subscriber{
			id:     "sub-key1",
			ctx:    t.Context(),
			cancel: func() {},
			keys:   []string{"key1"},
			handler: func(_ context.Context, e *configuration.UpdateEvent) error {
				mu.Lock()
				receivedByID["sub-key1"] = e
				mu.Unlock()
				return nil
			},
		})
		store.registry.add(&subscriber{
			id:     "sub-key2",
			ctx:    t.Context(),
			cancel: func() {},
			keys:   []string{"key2"},
			handler: func(_ context.Context, e *configuration.UpdateEvent) error {
				mu.Lock()
				receivedByID["sub-key2"] = e
				mu.Unlock()
				return nil
			},
		})

		oldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			Data:       map[string]string{"key1": "old1", "key2": "old2"},
		}
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"},
			Data:       map[string]string{"key1": "new1", "key2": "old2"}, // only key1 changed
		}

		store.fanOutUpdate(oldCM, newCM)

		assert.Contains(t, receivedByID, "sub-key1")
		assert.NotContains(t, receivedByID, "sub-key2") // key2 didn't change
	})

	t.Run("skips cancelled subscribers", func(t *testing.T) {
		store := newTestStore(t)
		store.metadata = metadata{ConfigMapName: "my-config", Namespace: ptr.Of("default")}

		cancelledCtx, cancel := context.WithCancel(t.Context())
		cancel() // cancel immediately

		handlerCalled := false
		store.registry.add(&subscriber{
			id:     "cancelled-sub",
			ctx:    cancelledCtx,
			cancel: cancel,
			keys:   []string{},
			handler: func(_ context.Context, _ *configuration.UpdateEvent) error {
				handlerCalled = true
				return nil
			},
		})

		oldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			Data:       map[string]string{"key1": "old"},
		}
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"},
			Data:       map[string]string{"key1": "new"},
		}

		store.fanOutUpdate(oldCM, newCM)

		assert.False(t, handlerCalled)
	})

	t.Run("non-ConfigMap objects are ignored", func(t *testing.T) {
		store := newTestStore(t)
		store.metadata = metadata{ConfigMapName: "my-config", Namespace: ptr.Of("default")}

		handlerCalled := false
		store.registry.add(&subscriber{
			id:     "sub-1",
			ctx:    t.Context(),
			cancel: func() {},
			keys:   []string{},
			handler: func(_ context.Context, _ *configuration.UpdateEvent) error {
				handlerCalled = true
				return nil
			},
		})

		store.fanOutUpdate("not-a-configmap", "not-a-configmap")

		assert.False(t, handlerCalled)
	})

	t.Run("same ResourceVersion is skipped", func(t *testing.T) {
		store := newTestStore(t)
		store.metadata = metadata{ConfigMapName: "my-config", Namespace: ptr.Of("default")}

		handlerCalled := false
		store.registry.add(&subscriber{
			id:     "sub-1",
			ctx:    t.Context(),
			cancel: func() {},
			keys:   []string{},
			handler: func(_ context.Context, _ *configuration.UpdateEvent) error {
				handlerCalled = true
				return nil
			},
		})

		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			Data:       map[string]string{"key1": "val"},
		}

		store.fanOutUpdate(cm, cm)

		assert.False(t, handlerCalled)
	})

	t.Run("data unchanged is skipped despite different ResourceVersion", func(t *testing.T) {
		store := newTestStore(t)
		store.metadata = metadata{ConfigMapName: "my-config", Namespace: ptr.Of("default")}

		handlerCalled := false
		store.registry.add(&subscriber{
			id:     "sub-1",
			ctx:    t.Context(),
			cancel: func() {},
			keys:   []string{},
			handler: func(_ context.Context, _ *configuration.UpdateEvent) error {
				handlerCalled = true
				return nil
			},
		})

		oldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			Data:       map[string]string{"key1": "val"},
		}
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"},
			Data:       map[string]string{"key1": "val"},
		}

		store.fanOutUpdate(oldCM, newCM)

		assert.False(t, handlerCalled)
	})

	t.Run("concurrent dispatch to subscribers", func(t *testing.T) {
		store := newTestStore(t)
		store.metadata = metadata{ConfigMapName: "my-config", Namespace: ptr.Of("default")}

		var maxConcurrent atomic.Int32
		var currentConcurrent atomic.Int32
		doneCh := make(chan struct{})

		for i := range 5 {
			id := fmt.Sprintf("sub-%d", i)
			store.registry.add(&subscriber{
				id:     id,
				ctx:    t.Context(),
				cancel: func() {},
				keys:   []string{},
				handler: func(_ context.Context, _ *configuration.UpdateEvent) error {
					cur := currentConcurrent.Add(1)
					for {
						old := maxConcurrent.Load()
						if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
							break
						}
					}
					<-doneCh
					currentConcurrent.Add(-1)
					return nil
				},
			})
		}

		go func() {
			oldCM := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
				Data:       map[string]string{"key1": "old"},
			}
			newCM := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"},
				Data:       map[string]string{"key1": "new"},
			}
			store.fanOutUpdate(oldCM, newCM)
		}()

		// Wait for at least 2 handlers to be running concurrently.
		require.Eventually(t, func() bool {
			return maxConcurrent.Load() >= 2
		}, 2*time.Second, 10*time.Millisecond)

		close(doneCh)
	})
}

func TestSubscribe_InitialState(t *testing.T) {
	cm := testConfigMap()
	store := newTestStore(t, cm)
	store.metadata = metadata{ConfigMapName: "my-config", Namespace: ptr.Of("default")}

	receivedCh := make(chan *configuration.UpdateEvent, 1)
	_, err := store.Subscribe(t.Context(), &configuration.SubscribeRequest{
		Keys:     []string{"log.level"},
		Metadata: map[string]string{},
	}, func(_ context.Context, e *configuration.UpdateEvent) error {
		receivedCh <- e
		return nil
	})
	require.NoError(t, err)

	select {
	case received := <-receivedCh:
		require.NotNil(t, received)
		assert.Equal(t, "info", received.Items["log.level"].Value)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for initial state delivery")
	}

	require.NoError(t, store.Close())
}

func TestSubscriptionRegistry(t *testing.T) {
	t.Run("add and remove by ID", func(t *testing.T) {
		r := newSubscriptionRegistry()
		r.add(&subscriber{
			id:     "s1",
			keys:   []string{"key1"},
			ctx:    t.Context(),
			cancel: func() {},
			handler: func(_ context.Context, _ *configuration.UpdateEvent) error {
				return nil
			},
		})

		assert.Contains(t, r.byID, "s1")
		assert.Contains(t, r.byKey["key1"], "s1")

		sub, ok := r.remove("s1")
		assert.True(t, ok)
		assert.NotNil(t, sub)
		assert.Empty(t, r.byID)
		assert.Empty(t, r.byKey)
	})

	t.Run("all-keys subscriber indexed in allKeys", func(t *testing.T) {
		r := newSubscriptionRegistry()
		r.add(&subscriber{
			id:     "s1",
			keys:   []string{},
			ctx:    t.Context(),
			cancel: func() {},
			handler: func(_ context.Context, _ *configuration.UpdateEvent) error {
				return nil
			},
		})

		assert.Contains(t, r.allKeys, "s1")
		assert.Empty(t, r.byKey)

		r.remove("s1")
		assert.Empty(t, r.allKeys)
	})

	t.Run("cancelAll cancels all subscribers", func(t *testing.T) {
		r := newSubscriptionRegistry()
		var count atomic.Int32
		for i := range 3 {
			id := fmt.Sprintf("s%d", i)
			r.add(&subscriber{
				id:     id,
				keys:   []string{"k"},
				ctx:    t.Context(),
				cancel: func() { count.Add(1) },
				handler: func(_ context.Context, _ *configuration.UpdateEvent) error {
					return nil
				},
			})
		}

		r.cancelAll()
		assert.Equal(t, int32(3), count.Load())
		assert.Empty(t, r.byID)
		assert.Empty(t, r.byKey)
	})

	t.Run("key index maps multiple subscribers to same key", func(t *testing.T) {
		r := newSubscriptionRegistry()
		for _, id := range []string{"s1", "s2"} {
			r.add(&subscriber{
				id:     id,
				keys:   []string{"shared-key"},
				ctx:    t.Context(),
				cancel: func() {},
				handler: func(_ context.Context, _ *configuration.UpdateEvent) error {
					return nil
				},
			})
		}

		assert.Len(t, r.byKey["shared-key"], 2)
		assert.Contains(t, r.byKey["shared-key"], "s1")
		assert.Contains(t, r.byKey["shared-key"], "s2")

		r.remove("s1")
		assert.Len(t, r.byKey["shared-key"], 1)
		assert.Contains(t, r.byKey["shared-key"], "s2")
	})

	t.Run("remove non-existent returns false", func(t *testing.T) {
		r := newSubscriptionRegistry()
		_, ok := r.remove("nonexistent")
		assert.False(t, ok)
	})
}
