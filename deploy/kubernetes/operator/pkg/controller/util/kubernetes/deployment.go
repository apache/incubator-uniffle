package kubernetes

import (
	"context"
	"encoding/json"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/utils"
)

// SyncDeployments synchronizes deployment objects.
func SyncDeployments(kubeClient kubernetes.Interface, deployments []*appsv1.Deployment) error {
	for i := range deployments {
		if err := SyncDeployment(kubeClient, deployments[i]); err != nil {
			return err
		}
	}
	return nil
}

// SyncDeployment synchronizes deployment object.
func SyncDeployment(kubeClient kubernetes.Interface, deploy *appsv1.Deployment) error {
	oldDeploy, err := kubeClient.AppsV1().Deployments(deploy.Namespace).
		Get(context.Background(), deploy.Name, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			klog.Errorf("get deployment (%v) failed: %v", utils.UniqueName(deploy), err)
			return err
		}
		// try to create a new deployment.
		if _, err = kubeClient.AppsV1().Deployments(deploy.Namespace).
			Create(context.Background(), deploy, metav1.CreateOptions{}); err != nil {
			klog.Errorf("create deployment (%v) failed: %v", utils.UniqueName(deploy), err)
			return err
		}
		return nil
	}
	// if it already exists, try to update it through patch method.
	return PatchDeployment(kubeClient, oldDeploy, deploy)
}

// PatchDeployment patches the old deployment to new deployment.
func PatchDeployment(kubeClient kubernetes.Interface,
	oldDeploy, newDeploy *appsv1.Deployment) error {
	var oldData []byte
	var err error
	oldData, err = json.Marshal(oldDeploy)
	if err != nil {
		klog.Errorf("marshal oldDeploy (%+v) failed: %v", oldDeploy, err)
		return err
	}
	var newData []byte
	newData, err = json.Marshal(newDeploy)
	if err != nil {
		klog.Errorf("marshal newDeploy (%+v) failed: %v", newDeploy, err)
		return err
	}
	// build payload for patch method.
	var patchBytes []byte
	patchBytes, err = strategicpatch.CreateTwoWayMergePatch(oldData, newData,
		&appsv1.Deployment{})
	if err != nil {
		klog.Errorf("created merge patch for deployment %v failed: %v",
			utils.UniqueName(oldDeploy), err)
		return err
	}
	if _, err = kubeClient.AppsV1().Deployments(oldDeploy.Namespace).Patch(context.Background(),
		oldDeploy.Name, types.StrategicMergePatchType, patchBytes,
		metav1.PatchOptions{}); err != nil {
		klog.Errorf("patch deployment (%v) failed: %v", utils.UniqueName(oldDeploy), err)
		return err
	}
	return nil
}
