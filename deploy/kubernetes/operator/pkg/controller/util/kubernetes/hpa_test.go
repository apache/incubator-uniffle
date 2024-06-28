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
	"testing"

	"github.com/stretchr/testify/assert"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

const (
	testHPAName      = "test"
	testHPANamespace = "test"
	testMaxReplicas  = 3
)

func buildTestHPA() *autoscalingv2.HorizontalPodAutoscaler {
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testHPAName,
			Namespace: testHPANamespace,
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			MaxReplicas: testMaxReplicas,
		},
	}
}

func TestRemoveHPA(t *testing.T) {
	assertion := assert.New(t)
	testHPA := buildTestHPA()
	kubeClient := kubefake.NewSimpleClientset(testHPA)
	assertion.Empty(RemoveHPA(kubeClient, testHPA))
	curHPA, err := kubeClient.AutoscalingV2().HorizontalPodAutoscalers(testHPA.Namespace).
		Get(context.TODO(), testHPA.Name, metav1.GetOptions{})
	assertion.Empty(curHPA)
	assertion.Equal(errors.IsNotFound(err), true)
}

func TestPatchHPA(t *testing.T) {
	assertion := assert.New(t)
	oldHPA := buildTestHPA()
	kubeClient := kubefake.NewSimpleClientset(oldHPA)
	newHPA := buildTestHPA()
	newHPA.Spec.MaxReplicas = 5
	assertion.Empty(PatchHPA(kubeClient, oldHPA, newHPA))
	curHPA, err := kubeClient.AutoscalingV2().HorizontalPodAutoscalers(oldHPA.Namespace).
		Get(context.TODO(), oldHPA.Name, metav1.GetOptions{})
	assertion.Empty(err)
	assertion.Equal(curHPA, newHPA)
}

func TestEnsureHPA(t *testing.T) {
	assertion := assert.New(t)
	kubeClient := kubefake.NewSimpleClientset()

	// try to create a hpa.
	initHPA := buildTestHPA()
	assertion.Empty(EnsureHPA(kubeClient, initHPA))
	curHPA, err := kubeClient.AutoscalingV2().HorizontalPodAutoscalers(initHPA.Namespace).
		Get(context.TODO(), initHPA.Name, metav1.GetOptions{})
	assertion.Empty(err)
	assertion.Equal(curHPA, initHPA)

	// try to update the hpa
	initHPA.Spec.MaxReplicas = 5
	assertion.Empty(EnsureHPA(kubeClient, initHPA))
	curHPA, err = kubeClient.AutoscalingV2().HorizontalPodAutoscalers(initHPA.Namespace).
		Get(context.TODO(), initHPA.Name, metav1.GetOptions{})
	assertion.Empty(err)
	assertion.Equal(curHPA, initHPA)
}

func TestSyncHPA(t *testing.T) {
	assertion := assert.New(t)
	kubeClient := kubefake.NewSimpleClientset()

	// try to create a hpa.
	initHPA := buildTestHPA()
	assertion.Empty(SyncHPA(kubeClient, initHPA, true))
	curHPA, err := kubeClient.AutoscalingV2().HorizontalPodAutoscalers(initHPA.Namespace).
		Get(context.TODO(), initHPA.Name, metav1.GetOptions{})
	assertion.Empty(err)
	assertion.Equal(curHPA, initHPA)

	// try to delete the hpa
	assertion.Empty(SyncHPA(kubeClient, initHPA, false))
	curHPA, err = kubeClient.AutoscalingV2().HorizontalPodAutoscalers(initHPA.Namespace).
		Get(context.TODO(), initHPA.Name, metav1.GetOptions{})
	assertion.Empty(curHPA)
	assertion.Equal(errors.IsNotFound(err), true)
}
