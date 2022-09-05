package kubernetes

import (
	"context"
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/utils"
)

// SyncConfigMap synchronizes configMap object.
func SyncConfigMap(kubeClient kubernetes.Interface, cm *corev1.ConfigMap) error {
	oldCM, err := kubeClient.CoreV1().ConfigMaps(cm.Namespace).
		Get(context.Background(), cm.Name, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			klog.Errorf("get configMap (%v) failed: %v", utils.UniqueName(cm), err)
			return err
		}
		// try to create a new configMap.
		if _, err = kubeClient.CoreV1().ConfigMaps(cm.Namespace).
			Create(context.Background(), cm, metav1.CreateOptions{}); err != nil {
			klog.Errorf("create configMap (%v) failed: %v", utils.UniqueName(cm), err)
			return err
		}
		return nil
	}
	// if it already exists, try to update it through patch method.
	return PatchConfigMap(kubeClient, oldCM, cm)
}

// PatchConfigMap patches the old configMap to new configMap.
func PatchConfigMap(kubeClient kubernetes.Interface,
	oldCM, newCM *corev1.ConfigMap) error {
	var oldData []byte
	var err error
	oldData, err = json.Marshal(oldCM)
	if err != nil {
		klog.Errorf("marshal oldCM (%+v) failed: %v", oldCM, err)
		return err
	}
	var newData []byte
	newData, err = json.Marshal(newCM)
	if err != nil {
		klog.Errorf("marshal newCM (%+v) failed: %v", newCM, err)
		return err
	}
	// build payload for patch method.
	var patchBytes []byte
	patchBytes, err = strategicpatch.CreateTwoWayMergePatch(oldData, newData,
		&corev1.ConfigMap{})
	if err != nil {
		klog.Errorf("created merge patch for configMap %v failed: %v", utils.UniqueName(oldCM), err)
		return err
	}
	if _, err = kubeClient.CoreV1().ConfigMaps(oldCM.Namespace).Patch(context.Background(),
		oldCM.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}); err != nil {
		klog.Errorf("patch configMap (%v) failed: %v", utils.UniqueName(oldCM), err)
		return err
	}
	return nil
}
