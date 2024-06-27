/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kubernetes

import (
	"context"
	"encoding/json"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/utils"
)

// SyncHPA synchronizes a hpa object.
func SyncHPA(kubeClient kubernetes.Interface, hpa *autoscalingv2.HorizontalPodAutoscaler, enable bool) error {
	if enable {
		return EnsureHPA(kubeClient, hpa)
	}
	return RemoveHPA(kubeClient, hpa)
}

// EnsureHPA ensures the hpa object with expected data.
func EnsureHPA(kubeClient kubernetes.Interface, hpa *autoscalingv2.HorizontalPodAutoscaler) error {
	oldHPA, err := kubeClient.AutoscalingV2().HorizontalPodAutoscalers(hpa.Namespace).
		Get(context.TODO(), hpa.Name, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			klog.Errorf("failed to get hpa (%v): %v", utils.UniqueName(hpa), err)
			return err
		}
		// try to create a new hpa.
		if _, err = kubeClient.AutoscalingV2().HorizontalPodAutoscalers(hpa.Namespace).
			Create(context.Background(), hpa, metav1.CreateOptions{}); err != nil {
			klog.Errorf("failed to create hpa (%v): %v", utils.UniqueName(hpa), err)
			return err
		}
		return nil
	}
	return PatchHPA(kubeClient, oldHPA, hpa)
}

// RemoveHPA removes the hpa object.
func RemoveHPA(kubClient kubernetes.Interface, hpa *autoscalingv2.HorizontalPodAutoscaler) error {
	err := kubClient.AutoscalingV2().HorizontalPodAutoscalers(hpa.Namespace).
		Delete(context.TODO(), hpa.Name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// PatchHPA patches the old hpa to new hpa.
func PatchHPA(kubeClient kubernetes.Interface,
	oldHPA, newHPA *autoscalingv2.HorizontalPodAutoscaler) error {
	var oldData []byte
	var err error
	oldData, err = json.Marshal(oldHPA)
	if err != nil {
		klog.Errorf("failed to marshal oldHPA (%+v): %v", oldHPA, err)
		return err
	}
	var newData []byte
	newData, err = json.Marshal(newHPA)
	if err != nil {
		klog.Errorf("failed to marshal newHPA (%+v): %v", newHPA, err)
		return err
	}
	// build payload for patch method.
	var patchBytes []byte
	patchBytes, err = strategicpatch.CreateTwoWayMergePatch(oldData, newData,
		&autoscalingv2.HorizontalPodAutoscaler{})
	if err != nil {
		klog.Errorf("failed to create merge patch for hpa %v: %v", utils.UniqueName(oldHPA), err)
		return err
	}
	if _, err = kubeClient.AutoscalingV2().HorizontalPodAutoscalers(oldHPA.Namespace).Patch(context.Background(),
		oldHPA.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}); err != nil {
		klog.Errorf("failed to patch hpa (%v) with (%v): %v",
			utils.UniqueName(oldHPA), string(patchBytes), err)
		return err
	}
	return nil
}
